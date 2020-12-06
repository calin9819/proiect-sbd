import pymongo
from pprint import pprint
import pandas as pd
import flask
from flask import request, jsonify
import json


conn = pymongo.MongoClient("mongodb://master:stud1234@37.120.249.57:27017/?authSource=daune_leasing&authMechanism=SCRAM-SHA-256")
db = conn["daune_leasing"]
print(db.list_collection_names())

clienti_leasing = db["clienti_leasing"]
clienti_daune = db["clienti_daune"]

#cerinta 1 - interogari simple

#1
projection={"_id":0,
            "NUME_CLIENT":1,
            "PROFESIA":1,
            "VARSTA":1,
            "SUMA_DEPOZIT":1}

cursor=clienti_leasing.find({},projection=projection)
set_clienti=list(cursor)
#pprint (set_clienti)
cursor.close()

#2
projection={"_id":0,
            "DAUNA":1,
            "VALOARE_DAUNA":1}
sort=[("VALOARE_DAUNA", -1)]
cursor=clienti_daune.find({}, projection=projection, sort=sort);
set_clienti=list(cursor)
#pprint (set_clienti)
cursor.close()

#3
projection={"_id":0,
            "TARAPRODUCATOR":1,
            "REGIUNEPRODUCATOR":1,
            "MARCA":1,
            "MODEL":1,
            "VALOARE_DAUNA":1
            }
sort=[("MARCA", 1)]
cursor=clienti_daune.find({}, projection=projection, sort=sort);
set_clienti=list(cursor)
#pprint (set_clienti)
cursor.close()

#4
projection={"_id":0,
            "NUME_CLIENT":1,
            "PROFESIA":1,
            "SUMA_SOLICITATA":1}
sort=[("VARSTA", 1)]
cursor=clienti_leasing.find({"FIDELITATE": 2},projection=projection, sort=sort)
set_clienti=list(cursor)
#pprint (set_clienti)
cursor.close()

#5
projection={"_id":0,
            "NUME_CLIENT":1,
            "PROFESIA":1,
            "SUMA_DEPOZIT":1}
cursor=clienti_leasing.find({"SUMA_DEPOZIT": {"$gt": 100000}},projection=projection)
set_clienti_depozit_100000=list(cursor)
#print("Clienti Leasing cu depozitul mai mare de 100000 \n\n")
#pprint(set_clienti_depozit_100000)
#print("\n\nNumarul clientilor cu depozitul mai mare de 100000 este: ", len(set_clienti_depozit_100000))
cursor.close()

#cerinta 1 - agregari

#1
pipeline=[{'$group' : {
           "_id" : "$PROFESIA",
           "VAL_CREDIT_TOTAL": { "$sum": "$VAL_CREDITE_RON" }
            }},
        { '$sort': {"_id": 1}}]
cursor=clienti_leasing.aggregate(pipeline)
df = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint(df)

#2
pipeline=[{ "$match": { "VARSTA": { "$gt": 30, "$lt": 40 } } },
{ "$group": { "_id": "$STARE_CIVILA", "NUMAR": { "$sum": 1 }, "MEDIE": { "$avg": "$SUMA_SOLICITATA" } } }]
cursor=clienti_leasing.aggregate(pipeline)
df = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint(df)

#3
pipeline=[
    {
        "$bucket": {
            "groupBy": "$SUMA_SOLICITATA",
            "boundaries": [0, 1000, 5000, 10000, 20000, 50000],
            "default": "Other",
            "output": {
                "Nr_total": { "$sum": 1 },
                "Clienti leasing": { "$push": { "NUME_CLIENT": "$NUME_CLIENT", "MONEDA": "$MONEDA", "VARSTA": "$VARSTA", "SUMA_SOLICITATA": "$SUMA_SOLICITATA" } }
            }
        }
    }
]
cursor=clienti_leasing.aggregate(pipeline)
df = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint(df)

#4
pipeline=[{
        "$facet": {
            "Starea civila":
                [{
                    "$unwind": "$STARE_CIVILA"
                }, { "$sortByCount": "$STARE_CIVILA" }],
            "Sex": [{
                    "$unwind": "$SEX"
                }, { "$sortByCount": "$SEX" }]
        }
    }]
cursor=clienti_leasing.aggregate(pipeline)
df = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint(df)

#5
pipeline=[{'$group' : {
           "_id" : "$PROFESIA",
            "VARSTA_MEDIE": { "$avg": "$VARSTA" }
            }},
        { '$sort': {"_id": 1}}]
cursor=clienti_leasing.aggregate(pipeline)
df = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint(df)

#cerinta 2

#1
projection = {"_id": 0,
              "AN_FABRICATIE": 1,
              "MARCA": 1,
              "COMPONENTA": 1,
              "PRET_MANOPERA": 1
              }
sort = [("MARCA", 1)]
cursor = clienti_daune.find({}, projection=projection, sort=sort)
df  = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint (df.loc[(df['AN_FABRICATIE'] < 2010) & (df['COMPONENTA'] =='BATTERY'), 'PRET_MANOPERA'])
df.loc[(df['AN_FABRICATIE'] < 2010) & (df['COMPONENTA'] =='BATTERY'), 'PRET_MANOPERA']=df.loc[(df['AN_FABRICATIE'] < 2010) & (df['COMPONENTA'] =='BATTERY'), 'PRET_MANOPERA']*1.10
#pprint(df.loc[(df['AN_FABRICATIE'] < 2010) & (df['COMPONENTA'] =='BATTERY'), 'PRET_MANOPERA'])

#2
projection={"_id":0,
            "NUME_CLIENT":1,
            "PROFESIA":1,
            "VARSTA":1,
            "SUMA_DEPOZIT":1}
cursor = clienti_leasing.find({}, projection=projection)
df = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint (df.loc[(df['PROFESIA'] =='Pensionar'), 'SUMA_DEPOZIT'])
df.loc[(df['PROFESIA'] =='Pensionar'), 'SUMA_DEPOZIT']=df.loc[(df['PROFESIA'] =='Pensionar'), 'SUMA_DEPOZIT']*1.25
#pprint(df.loc[(df['PROFESIA'] =='Pensionar'), 'SUMA_DEPOZIT'])

#3
projection={"_id":0,
            "NUME_CLIENT":1,
            "PROFESIA":1,
            "VARSTA":1,
            "SUMA_DEPOZIT":1}
cursor = clienti_leasing.find({}, projection=projection)
df = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint(df.loc[(df['NUME_CLIENT'] =='Harpa Constantin'), 'VARSTA'])
df.loc[(df['NUME_CLIENT'] =='Harpa Constantin'), 'VARSTA']=df.loc[(df['NUME_CLIENT'] =='Harpa Constantin'), 'VARSTA']+1
#pprint(df.loc[(df['NUME_CLIENT'] =='Harpa Constantin'), 'VARSTA'])

#4
projection={"_id":0,
            "NUME_CLIENT":1,
            "FIDELITATE":1,
            "SUMA_DEPOZIT":1}
cursor = clienti_leasing.find({}, projection=projection)
df = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint(df.loc[(df['SUMA_DEPOZIT'] > 100), 'FIDELITATE'])
df.loc[(df['SUMA_DEPOZIT'] > 100), 'FIDELITATE']=df.loc[(df['SUMA_DEPOZIT'] > 100), 'FIDELITATE']+1
#pprint(df.loc[(df['SUMA_DEPOZIT'] > 100), 'FIDELITATE'])

#5
projection={"_id":0,
            "NUME_CLIENT":1,
            "PRESCORING":1,
            "VARSTA":1}
cursor = clienti_leasing.find({}, projection=projection)
df = pd.DataFrame.from_dict(list(cursor))
cursor.close()
#pprint(df.loc[(df['VARSTA'] < 35), 'PRESCORING'])
df.loc[(df['VARSTA'] < 35), 'PRESCORING']=6
#pprint(df.loc[(df['VARSTA'] < 35), 'PRESCORING'])


#cerinta 3 => Returnarea rezultatului prelucrarii intr-o pagina Web (JSON)

app = flask.Flask(__name__)
app.config["DEBUG"] = False

#home page

@app.route('/', methods = ['GET'])

def home() :
    return  '''<h1>Regasirea clientilor cu leasing /h1>
<p>API pentru afisarea clientilor</p>'''

@app.errorhandler(404)

def page_not_found(e):
    return "<h1>404</h1><p> The requested route could not be accessed.</p>", 404


# pagina: http://127.0.0.1:5000/api/clienti_leasing?depozit=100000

@app.route('/api/clienti_leasing', methods=['GET'])
def api_clienti_leasing():
    # Verificarea parametrului introdus in URL.
    if 'depozit' in request.args:
        prag_depozit = int(request.args['depozit'])
    else:
        return "Error: Nu a fost precizata valoarea minima a depozitului pentru prelucrarea clientilor."


# regasirea datelor din MongoDB

    projection = {"_id": 0,
                  "NUME_CLIENT": 1,
                  "PROFESIA": 1,
                  "SUMA_DEPOZIT": 1}

    cursor=clienti_leasing.find({"SUMA_DEPOZIT": {"$gt": prag_depozit}},projection=projection)

    dataframe_clienti = pd.DataFrame.from_dict(list(cursor))

    cursor.close()

    rezultate_json = json.loads(dataframe_clienti.to_json(orient='records'))

    results_str = json.dumps(rezultate_json, sort_keys=True)

    results_str = results_str[1:-1]


    new_Data = ''
    for client in results_str:
        new_Data += client
        new_Data += '\n'


    print(new_Data)

    return '''<h1>Regasirea clientilor cu leasing si valoarea depozitului mai mare de ''' + str(prag_depozit) + '''</h1><p>API pentru afisarea clientilor</p>''' + new_Data
    #return jsonify(new_data)

app.run()



#cerinta 4 => Set de rapoarte pentru analiza colectiei MongoDB, utilizand PowerBI







