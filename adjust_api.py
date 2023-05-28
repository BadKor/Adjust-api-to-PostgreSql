import pprint
import requests
import json
import ast
import psycopg2
import pandas as pd


def tracker (date, id_app, id_event):

    headers = {
        "Authorization": "Token token=YOUR ADJUST TOKEN"
    }
    #In url used utc_offset +3 Hours
    #If/Else. It's using for 2 way: 1. If you have app where lead=install alghoritm choose id_event=='install'; 2. If you lead!=install then alghoritm choose else
    if id_event=='install':
        url='https://api.adjust.com/kpis/v1/'+id_app+'?kpis=installs,clicks&start_date='+date+'&end_date='+date+'&utc_offset=+03:00&grouping=trackers,countries,os_names'
    else:
        url='https://api.adjust.com/kpis/v1/' + id_app + '?kpis=installs,clicks&event_kpis=' + id_event + '&start_date=' + date + '&end_date=' + date + '&utc_offset=+03:00&grouping=trackers,countries,os_names'
    response = requests.get(
        url=url,
        headers=headers
    )
    # Load data to json
    jsresp_tonormalize = json.loads(response.text)

    new_json_dict = {}
    new_json_list = []
    tracker_token = []

    # Scrapping data from json whom we get from adjust to our json with simple structure for writing to DataBase
    for i in range(len(jsresp_tonormalize['result_parameters']['trackers'])):

        new_json_dict["date"] = jsresp_tonormalize['result_parameters']['start_date']

        new_json_dict["app_name"] = jsresp_tonormalize['result_set']['name']
        new_json_dict["app_token"] = jsresp_tonormalize['result_set']['token']
        new_json_dict["tracker_name"] = jsresp_tonormalize['result_parameters']['trackers'][i]['name']
        new_json_dict["tracker_token"] = jsresp_tonormalize['result_parameters']['trackers'][i]['token']
        new_json_dict["install"] = 0
        new_json_dict["pay_action"] = 0
        for b in range(len(jsresp_tonormalize['result_set']['trackers'])):
            if jsresp_tonormalize['result_set']['trackers'][b]['token'] == jsresp_tonormalize['result_parameters']['trackers'][i]['token']:
                tracker_token.append(jsresp_tonormalize['result_parameters']['trackers'][i]['token'])
                for n in range(len(jsresp_tonormalize['result_set']['trackers'][b]['countries'])):
                    new_json_dict["geo"] = jsresp_tonormalize['result_set']['trackers'][b]['countries'][n]['country']

                    for k in range(len(jsresp_tonormalize['result_set']['trackers'][b]['countries'][n]['os_names'])):
                        new_json_dict["os_names"] = jsresp_tonormalize['result_set']['trackers'][b]['countries'][n]['os_names'][k]['os_name']
                        new_json_dict["install"] = jsresp_tonormalize['result_set']['trackers'][b]['countries'][n]['os_names'][k]['kpi_values'][0]
                        if id_event != 'install':

                            new_json_dict["pay_action"] = jsresp_tonormalize['result_set']['trackers'][b]['countries'][n]['os_names'][k]['kpi_values'][2]



                        new_json_list.append(str(new_json_dict))
                        new_json_list[-1] = ast.literal_eval(new_json_list[-1])
                break


    # Connect to DB
    conn = psycopg2.connect(dbname="dbname", host="10.10.10.10", user="user", password="password", port="9000")
    print("Connected to DB. Loading Trackers...")
    cursor = conn.cursor()

    # Insert data into table (table_name). Structure table you can see in Readme file
    query_sql = """insert into table_name 
                   select * from json_populate_recordset(NULL::table_name, %s) """
    cursor.execute(query_sql, (json.dumps(new_json_list),))
    conn.commit()
    conn.close()

    return tracker_token



def campaign (date, id_app, id_event, trackers):
    headers = {
        "Authorization": "Token token=YOUR ADJUST TOKEN"
    }
    campaign_token = []
    # Connect to DB before writing to DB because we have a cycle. And if we connect before cycle we can spent not more time for
    # connect to DB every iteration in cycle
    conn = psycopg2.connect(dbname="dbname", host="10.10.10.10", user="user", password="password", port="9000")
    print("Connected to DB. Loading Campaigns...")
    for k in range(len(trackers)):

        # In url used utc_offset +3 Hours
        # If/Else. It's using for 2 way: 1. If you have app where lead=install alghoritm choose id_event=='install'; 2. If you lead!=install then alghoritm choose else
        if id_event=='install':
            url='https://api.adjust.com/kpis/v1/'+id_app+'/trackers/'+trackers[k]+'?kpis=installs,clicks&start_date='+date+'&end_date='+date+'&utc_offset=+03:00&grouping=campaign,countries,os_names'
        else:
            url='https://api.adjust.com/kpis/v1/'+id_app+'/trackers/'+trackers[k]+'?kpis=installs,clicks&event_kpis='+id_event+'&start_date='+date+'&end_date='+date+'&utc_offset=+03:00&grouping=campaign,countries,os_names'
        response = requests.get(
            url=url,
            headers=headers
        )
        # Load data to json
        jsresp_tonormalize = json.loads(response.text)
        new_json_dict = {}
        new_json_list = []

        # Scrapping data from json whom we get from adjust to our json with simple structure for writing to DataBase
        for i in range(len(jsresp_tonormalize['result_set']['campaigns'])):
            if len(jsresp_tonormalize['result_set']['campaigns'][i]['token'])>1:
                campaign_token.append(jsresp_tonormalize['result_set']['campaigns'][i]['token'])
            new_json_dict["date"] = jsresp_tonormalize['result_parameters']['start_date']
            new_json_dict["tracker_name"] = jsresp_tonormalize['result_set']['name']
            new_json_dict["tracker_token"] = jsresp_tonormalize['result_set']['token']
            
            for n in range(len(jsresp_tonormalize['result_set']['campaigns'][i]['countries'])):
                new_json_dict["campaign_name"] = jsresp_tonormalize['result_set']['campaigns'][i]['name']
                new_json_dict["campaign_token"] = jsresp_tonormalize['result_set']['campaigns'][i]['token']
                new_json_dict["geo"] = jsresp_tonormalize['result_set']['campaigns'][i]['countries'][n]['country']
                
                for z in range (len(jsresp_tonormalize['result_set']['campaigns'][i]['countries'][n]['os_names'])):
                    new_json_dict["os_names"] = jsresp_tonormalize['result_set']['campaigns'][i]['countries'][n]['os_names'][z]['os_name']
                    new_json_dict["install"] = jsresp_tonormalize['result_set']['campaigns'][i]['countries'][n]['os_names'][z]['kpi_values'][0]
                    if id_event != 'install':
                        new_json_dict["pay_action"] = jsresp_tonormalize['result_set']['campaigns'][i]['countries'][n]['os_names'][z]['kpi_values'][2]



                    new_json_list.append(str(new_json_dict))
                    new_json_list[-1] = ast.literal_eval(new_json_list[-1])


        
        cursor = conn.cursor()

        # Insert data into table (table_name). Structure table you can see in Readme file
        query_sql = """insert into table_name 
                          select * from json_populate_recordset(NULL::table_name, %s) """
        cursor.execute(query_sql, (json.dumps(new_json_list),))

        conn.commit()
    
    #Close Connect DB
    conn.close()

    return campaign_token


def sub_id (date, id_app, id_event, campaign_tkn):
    headers = {
        "Authorization": "Token token=YOUR ADJUST TOKEN"
    }
    subid_token = []
    
    # Connect to DB before writing to DB because we have a cycle. And if we connect before cycle we can spent not more time for
    # connect to DB every iteration in cycle
    conn = psycopg2.connect(dbname="dbname", host="10.10.10.10", user="user", password="password", port="9000")
    print("Connected to DB. Loading sub_id....")
    for k in range(len(campaign_tkn)):
        # In url used utc_offset +3 Hours
        # If/Else. It's using for 2 way: 1. If you have app where lead=install alghoritm choose id_event=='install'; 2. If you lead!=install then alghoritm choose else
        if id_event == 'install':
            url='https://api.adjust.com/kpis/v1/'+id_app+'/trackers/'+campaign_tkn[k]+'?kpis=installs,clicks&start_date='+date+'&end_date='+date+'&utc_offset=+03:00'
        else:
            url='https://api.adjust.com/kpis/v1/'+id_app+'/trackers/'+campaign_tkn[k]+'?kpis=installs,clicks&event_kpis='+id_event+'&start_date='+date+'&end_date='+date+'&utc_offset=+03:00'
        response = requests.get(
            url=url,
            headers=headers
        )
        # Load data to json
        jsresp_tonormalize = json.loads(response.text)
        
        new_json_dict = {}
        new_json_list = []

        # Scrapping data from json whom we get from adjust to our json with simple structure for writing to DataBase
        for i in range(len(jsresp_tonormalize['result_parameters']['trackers'])):
            strk = jsresp_tonormalize['result_parameters']['trackers'][i]['name'].split("::")
            new_json_dict["date"] = jsresp_tonormalize['result_parameters']['start_date']
            new_json_dict["tracker_name"] = strk[0]
            new_json_dict["campaign_name"] = strk[1]
            new_json_dict["campaign_token"] =  "test"
            new_json_dict["sub_id"] = strk[2]
            new_json_dict["sub_id_token"] = jsresp_tonormalize['result_parameters']['trackers'][i]['token']
            new_json_dict["install"] = 0
            new_json_dict["pay_action"] = 0
            for b in range(len(jsresp_tonormalize['result_set']['trackers'])):
                if jsresp_tonormalize['result_set']['trackers'][b]['token'] == jsresp_tonormalize['result_parameters']['trackers'][i]['token']:
                    new_json_dict["install"] = jsresp_tonormalize['result_set']['trackers'][b]['kpi_values'][0]
                    if id_event != 'install':
                        new_json_dict["pay_action"] = jsresp_tonormalize['result_set']['trackers'][b]['kpi_values'][2]
                    break
            new_json_dict1 = str(new_json_dict)
            new_json_list.append(new_json_dict1)
            new_json_list[-1] = ast.literal_eval(new_json_list[i])


        
        cursor = conn.cursor()
        # Insert data into table (table_name). Structure table you can see in Readme file
        query_sql = """insert into table_name 
                           select * from json_populate_recordset(NULL::table_name, %s) """
        cursor.execute(query_sql, (json.dumps(new_json_list),))
        conn.commit()
    # Close Connect DB
    conn.close()

    return subid_token

def creo (date, id_app, id_event, subids):
    headers = {
        "Authorization": "Token token=YOUR ADJUST TOKEN"
    }
    
    # Connect to DB before writing to DB because we have a cycle. And if we connect before cycle we can spent not more time for
    # connect to DB every iteration in cycle
    conn = psycopg2.connect(dbname="dbname", host="10.10.10.10", user="user", password="password", port="9000")
    print("Connected to DB. Loading Creatives....")
    cursor = conn.cursor()

    for k in range(len(subids)):
        # In url used utc_offset +3 Hours
        # If/Else. It's using for 2 way: 1. If you have app where lead=install alghoritm choose id_event=='install'; 2. If you lead!=install then alghoritm choose else
        if id_event=='install':
            url='https://api.adjust.com/kpis/v1/'+id_app+'/trackers/'+subids[k]+'?kpis=installs,sessions&start_date='+date+'&end_date='+date+'&utc_offset=+03:00&grouping=creatives,countries'
        else:
            url='https://api.adjust.com/kpis/v1/'+id_app+'/trackers/'+subids[k]+'?kpis=installs,sessions&event_kpis='+id_event+'&start_date='+date+'&end_date='+date+'&utc_offset=+03:00&grouping=creatives,countries'

        response = requests.get(
            url=url,
            headers=headers
        )

        # Load data to json
        jsresp_tonormalize = json.loads(response.text)
        
        new_json_dict = {}
        new_json_list = []

        # Scrapping data from json whom we get from adjust to our json with simple structure for writing to DataBase
        strk = jsresp_tonormalize['result_set']['name'].split("::")
        for i in range(len(jsresp_tonormalize['result_set']['creatives'])):
            new_json_dict["date"] = jsresp_tonormalize['result_parameters']['start_date']
            new_json_dict["tracker_name"] = strk[0]
            new_json_dict["campaign_name"] = strk[1]
            new_json_dict["campaign_token"] =  "test"
            new_json_dict["sub_id"] = strk[2]
            new_json_dict["creative_name"] = jsresp_tonormalize['result_set']['creatives'][i]['name']
            new_json_dict["creative_token"]=jsresp_tonormalize['result_set']['creatives'][i]['token']

            for n in range(len(jsresp_tonormalize['result_set']['creatives'][i]['countries'])):
                new_json_dict["geo"]=jsresp_tonormalize['result_set']['creatives'][i]['countries'][n]['country']
                new_json_dict["install"] = jsresp_tonormalize['result_set']['creatives'][i]['countries'][n]['kpi_values'][0]
                if id_event != 'install':
                    new_json_dict["pay_action"] = jsresp_tonormalize['result_set']['creatives'][i]['countries'][n]['kpi_values'][2]

                
                new_json_list.append(str(new_json_dict))
                new_json_list[-1] = ast.literal_eval(new_json_list[-1])

        # Insert data into table (table_name). Structure table you can see in Readme file
        query_sql = """insert into table_name 
                               select * from json_populate_recordset(NULL::table_name, %s) """
        cursor.execute(query_sql, (json.dumps(new_json_list),))
        conn.commit()
    # Close Connect DB
    conn.close()




# You can use not one ids apps if you want. But if you want use not one app you need add cycle
id_app = 'Your App Id in Adjust'

Action_id = 'Your Event App in Adjust'


# Dates for load dates, you can use any cycle with iterations date. But I'm using simple list with dates for easy example
dates = ('2023-04-21','2023-04-22','2023-04-23','2023-04-24','2023-04-25','2023-04-26')

for k in range(len(dates)):

        trackers = tracker(date=dates[k], id_app=id_app, id_event=Action_id,)

        campaign_tkn = campaign(date=dates[k], id_app=id_app, id_event=Action_id, trackers=trackers)

        subids = sub_id(date=dates[k], id_app=id_app, id_event=Action_id, campaign_tkn=campaign_tkn)

        creo(date=dates[k], id_app=id_app, id_event=Action_id, subids=subids)
