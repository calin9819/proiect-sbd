import pymongo
from pprint import pprint
import pandas as pd
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
cursor=clienti_leasing.find({"SUMA_DEPOZIT": {"$gt": 10000}},projection=projection)
set_clienti=list(cursor)
#pprint (set_clienti)
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









