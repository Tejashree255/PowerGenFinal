import pandas as pd
import streamlit as st
import datetime
import os
import xlrd
import shutil
from sqlalchemy import create_engine
import pandas as pd
import warnings
import streamlit as st
from google.oauth2.service_account import Credentials
from gsheetsdb import connect
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import gspread
from gspread_dataframe import set_with_dataframe


scopes = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

credentials = Credentials.from_service_account_file('powerforecasting-823bf86ac870.json', scopes=scopes)

gc = gspread.authorize(credentials)

gauth = GoogleAuth()
drive = GoogleDrive(gauth)

# open a google sheet
gs = gc.open_by_url('https://docs.google.com/spreadsheets/d/1UM224VvciQoVYpQfKc533VwO28XjHrWrgwfNj669yMA/edit?usp=sharing')
# select a work sheet from its name
worksheet1 = gs.worksheet('one')
st.write(worksheet1)


df=pd.DataFrame({'date':'23-04-2023','Site_name':'Pune'})
set_with_dataframe(worksheet=worksheet1, dataframe=df, include_index=False,
include_column_header=True, resize=True)
st.write('Inserted!')
# Authenticate with Google Drive
# Create a connection object.
# credentials = service_account.Credentials.from_service_account_info(
#     st.secrets["gcp_service_account"],
#     scopes=[
#         "https://www.googleapis.com/auth/spreadsheets",
#     ],
# )
#conn = connect(credentials=credentials)

# credentials = service_account.Credentials.from_service_account_info(
#     st.secrets["gcp_service_account"],
#     scopes=[
      
#         "https://www.googleapis.com/auth/spreadsheets"
#     ],
# )
# creds = service_account.Credentials.from_service_account_file('powerforecasting-823bf86ac870.json', [
#         "https://www.googleapis.com/auth/spreadsheets"
#     ],subject="power-test@powerforecasting.iam.gserviceaccount.com"
#  )
# client = Client(scope=credentials.scopes,creds=credentials)
# sheetname="GenerationSheet"
# spread=Spread(sheetname,client=client)
# st.write(spread.url)
# sh=client.open(sheetname)
# wlist=sh.worksheets;
# st.write("Inserted")
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
st.subheader('Data Analysis')

# Print results.
#
warnings.filterwarnings("ignore")

#db_connection = create_engine('mysql+pymysql://root:purva@localhost:3306/TEIM_JSON5')             #change database name

#purva's file


def log_files(input_path):
    file_log = open('C:/Users/purva/Downloads/energy_forecasting-master/energy_forecasting-master/Logs/file_log.txt', 'a')

    files = [(name[0], filename) for name in os.walk(input_path) for filename in name[2]]

    for file in files:
        file_log.write(str(file) + '\t' + str(datetime.datetime.now()) + '\n')

    return files


def cleanup(input_path):
    destination = 'C:/Users/purva/Downloads/energy_forecasting-master/energy_forecasting-master/DGR/'
    clients = ['INOX', 'REGEN', 'SENVION', 'SUZLON', 'TS WIND', 'WIND WORLD']

    for client in clients:
        files = [(name[0], filename) for name in os.walk(os.path.join(input_path, client)) for filename in name[2]]

        for file in files:
            shutil.move(file[0] + '/' + file[1], destination + client + '/' + file[1])


def timestamp_to_hours(sheet, from_column, to_column):
    for x in range(len(sheet)):
        if len(str(sheet.loc[x, from_column])) > 8:
            sheet.loc[x, to_column] = 24.0
        elif len(str(sheet.loc[x, from_column])) == 8:
            temp = list(map(float, str(sheet.loc[x, from_column]).split(':')))
            sheet.loc[x, to_column] = temp[0] + temp[1] / 60 + temp[2] / 3600
        else:
            sheet.loc[x, to_column] = 0.0
    return sheet



def timestamp_to_hours2(sheet, column):
    for x in range(len(sheet)):
        temp = list(map(float, str(sheet.loc[x, column]).split(':')))
        sheet.loc[x, column] = temp[0] + temp[1] / 60
    return sheet

def add_time(time1, time2):
    l1 = list(map(int, time1.split(':')))
    l2 = list(map(int, time2.split(':')))
    minutes = (l1[1] + l2[1]) % 60
    hours = l1[0] + l2[0] + int((l1[1] + l2[1]) / 60)
    return str(hours) + ':' + str(minutes)


def get_suzlon_data(gen_data, bd_data, generation, breakdown):
   # input_path = 'C:/Users/purva/Downloads/energy_forecasting-master/energy_forecasting-master/DGR/SUZLON'

    #files = log_files(input_path)
    print('In')


    generation['Gen. Date'] = pd.to_datetime(generation['Gen. Date'])
    generation['date'] = generation['Gen. Date'].dt.date
    generation['financial_year'] = [
        'FY ' + str(y) + '-' + str(y + 1 - 2000) if x not in [1, 2, 3] else 'FY ' + str(y - 1) + '-' + str(y - 2000)
        for x, y in
        zip(generation['Gen. Date'].dt.month, generation['Gen. Date'].dt.year)]
    generation['client_name'] = ['Suzlon'] * len(generation)

    generation.replace('*', 0.0, inplace=True)
    generation.replace('**', 0.0, inplace=True)

    generation = generation.reset_index().rename(columns={'Customer Name': 'customer_name',
                                                            'State': 'state', 'Site': 'site_name',
                                                            'Loc. No.': 'wind_turbine_location_number',
                                                            'Gen. (kwh) DAY': 'day_generation_kwh',
                                                            'Gen Hrs.': 'day_generation_hours',
                                                            'Opr Hrs.': 'operating_hours',
                                                            'M/C Avail.%': 'machine_availability_percent',
                                                            '%PLF DAY': 'plant_load_factor',
                                                            'GF': 'grid_failure',
                                                            'FM': 'force_majeure',
                                                            'S': 'scheduled_services',
                                                            'U': 'unscheduled_services'})
    generation['grid_availability']=(24-generation['grid_failure'])/24 * 100
    generation = generation[['date', 'financial_year', 'customer_name', 'client_name',
                                'state', 'site_name', 'wind_turbine_location_number',
                                'day_generation_kwh', 'day_generation_hours',
                                'operating_hours', 'machine_availability_percent',
                                'plant_load_factor', 'grid_failure', 'force_majeure',
                                'scheduled_services', 'unscheduled_services','grid_availability']]
    if not breakdown.empty:
        breakdown['Gen. Date'] = pd.to_datetime(breakdown['Gen. Date'])
        breakdown['date'] = breakdown['Gen. Date'].dt.date
        breakdown['financial_year'] = [
            'FY ' + str(y) + '-' + str(y + 1 - 2000) if x not in [1, 2, 3] else 'FY ' + str(y - 1) + '-' + str(y - 2000)
            for x, y in
            zip(breakdown['Gen. Date'].dt.month, breakdown['Gen. Date'].dt.year)]
        breakdown['client_name'] = ['Suzlon'] * len(breakdown)

        breakdown = breakdown.reset_index().rename(columns={'Customer Name': 'customer_name',
                                                            'State': 'state', 'Site': 'site_name',
                                                            'Loc. No.': 'wind_turbine_location_number',
                                                            'Breakdown Remark': 'breakdown_remark',
                                                            'Breakdown Hrs.': 'breakdown_hours'})

        breakdown = breakdown[['date', 'financial_year', 'customer_name', 'client_name',
                                'state', 'site_name', 'wind_turbine_location_number',
                                'breakdown_remark', 'breakdown_hours']]

    gen_data = pd.concat([gen_data, generation], ignore_index=True)
    bd_data = pd.concat([bd_data, breakdown], ignore_index=True)
    print('*****',gen_data)
 
    return gen_data, bd_data


def get_windworld_data(gen_data, bd_data, sheet):

    sheet['DATE'] = pd.to_datetime(sheet['DATE'])
    sheet['date'] = sheet['DATE'].dt.date
    sheet['financial_year'] = [
        'FY ' + str(y) + '-' + str(y + 1 - 2000) if x not in [1, 2, 3] else 'FY ' + str(y - 1) + '-' + str(y - 2000)
        for x, y in
        zip(sheet['DATE'].dt.month, sheet['DATE'].dt.year)]
    sheet['client_name'] = ['Wind World'] * len(sheet)

    sheet = timestamp_to_hours(sheet, 'O.Hrs', 'day_generation_hours')


    bd_factors = {'BM': 'unscheduled_services', 'BD': 'unscheduled_services', 'GF': 'grid_failure',
                    'GS': 'grid_failure', 'PM': 'scheduled_services', 'SD': 'scheduled_services',
                    'FM': 'force_majeure', 'CS': 'grid_failure', 'LR': 'grid_failure', 'RF': 'force_majeure',
                    'STS': 'force_majeure', 'S/D': 'grid_failure', 'TE':'unscheduled_services','JF':'force_majeure','PH':'scheduled_services'}


    factors = ('BM', 'BD', 'GF', 'GS', 'PM', 'SD', 'FM', 'CS', 'LR', 'S/D', 'STS', 'RF', 'TE', 'JF', 'PH')

    sheet['grid_failure'] = ['00:00'] * len(sheet)
    sheet['force_majeure'] = ['00:00'] * len(sheet)
    sheet['scheduled_services'] = ['00:00'] * len(sheet)
    sheet['unscheduled_services'] = ['00:00'] * len(sheet)
    sheet['breakdown_hours'] = ['00:00'] * len(sheet)

    sheet.fillna('', inplace=True)
    # sheet = timestamp_to_hours2(sheet, 'L.Hrs')
    # sheet = timestamp_to_hours2(sheet, 'O.Hrs')
    
    for x in range(len(sheet)):

        remark = sheet.loc[x, 'REMARKS']
        count = remark.lower().count('hrs')
        if count > 0:
            for z in range(count):
                index = remark.lower().find('hrs')
                time_str = remark[index - 6:index]
                time_str = time_str.replace('.', ':')
                i = time_str.find(':')
                if i == -1:
                    time = time_str.strip()[-2:] + ':00'
                elif time_str[i - 2:i].isnumeric():
                    time = time_str[i - 2:i + 3]
                else:
                    time = time_str[i - 1:i + 3]
                if z == 0 and not remark[:1].isdigit():
                    sheet.loc[x, 'breakdown_hours'] = add_time(sheet.loc[x, 'breakdown_hours'], time)
                    if remark[:3] == 'STS':
                        last_factor = remark[:3]
                        sheet.loc[x, 'force_majeure'] = add_time(sheet.loc[x, 'force_majeure'], time)
                    elif remark[:3] == 'S/D':
                        last_factor = remark[:3]
                        sheet.loc[x, 'grid_failure'] = add_time(sheet.loc[x, 'grid_failure'], time)
                    else:
                        last_factor = remark[:2]
                        # print(last_factor)
                        sheet.loc[x, bd_factors[remark[:2]]] = add_time(sheet.loc[x, bd_factors[remark[:2]]], time)
                else:
                    flag = 0
                    for f in factors:
                        if remark.find(f + ' -', 0, index) != -1 or remark.find(f + '-', 0, index) != -1:
                            sheet.loc[x, 'breakdown_hours'] = add_time(sheet.loc[x, 'breakdown_hours'], time)
                            sheet.loc[x, bd_factors[f]] = add_time(sheet.loc[x, bd_factors[f]], time)
                            last_factor = f
                            flag = 1
                            break
                    if flag == 0:
                        sheet.loc[x, 'breakdown_hours'] = add_time(sheet.loc[x, 'breakdown_hours'], time)
                        sheet.loc[x, bd_factors[last_factor]] = add_time(sheet.loc[x, bd_factors[last_factor]], time)
                remark = remark[index + 3:]

    sheet = timestamp_to_hours2(sheet, 'grid_failure')
    sheet = timestamp_to_hours2(sheet, 'force_majeure')
    sheet = timestamp_to_hours2(sheet, 'scheduled_services')
    sheet = timestamp_to_hours2(sheet, 'unscheduled_services')
    sheet = timestamp_to_hours2(sheet, 'breakdown_hours')

    sheet = sheet.reset_index().rename(columns={'Customer': 'customer_name',
                                                'STATE ': 'state', 'SITE': 'site_name',
                                                'WEC': 'wind_turbine_location_number',
                                                'GENERATION': 'day_generation_kwh',
                                                'MA ': 'machine_availability_percent',
                                                'GIA': 'internal_grid_availability_percent',
                                                'CF': 'plant_load_factor', 'REMARKS': 'breakdown_remark','GA':'grid_availability'})

    generation = sheet[['date', 'financial_year', 'customer_name', 'client_name',
                        'state', 'site_name', 'wind_turbine_location_number',
                        'day_generation_kwh','day_generation_hours',   
                        'machine_availability_percent', 'internal_grid_availability_percent',
                        'plant_load_factor', 'grid_failure', 'force_majeure',
                        'scheduled_services', 'unscheduled_services','grid_availability']]

    breakdown = sheet[['date', 'financial_year', 'customer_name', 'client_name',
                        'state', 'site_name', 'wind_turbine_location_number',
                        'breakdown_remark', 'breakdown_hours']]

    breakdown = breakdown[breakdown['breakdown_remark'] != '']

    gen_data = pd.concat([gen_data, generation], ignore_index=True)
    bd_data = pd.concat([bd_data, breakdown], ignore_index=True)

    return gen_data, bd_data


def consolidate():
    #create_tables()

    input_files = st.file_uploader("Upload input file", accept_multiple_files=True)
    


    dataS='Gen. Date'
    dataW='WEC Wise Report'

    for f in input_files:
        #workbook = openpyxl.load_workbook(f, read_only=True, keep_links=False)
        #data = pd.read_excel(f, sheet_name=workbook.sheetnames[0])
        gen_data = pd.DataFrame()
        bd_data = pd.DataFrame()
       
        file = pd.ExcelFile(f)
        list_of_dfs = []
        for sheet in file.sheet_names:    
            # Parse data from each worksheet as a Pandas DataFrame
            df = file.parse(sheet)

            # And append it to the list
            list_of_dfs.append(df)
    
        #print(list_of_dfs)
        
       
        if len(list_of_dfs) == 2:
                generation = list_of_dfs[0]
                print('generation: ', generation)
                breakdown = list_of_dfs[1]
                print('breakdown',breakdown)
                gen_data, bd_data = get_suzlon_data(gen_data, bd_data, generation, breakdown)
                print('Done suzlon')
                print('gen data',gen_data)
        elif len(list_of_dfs) == 1:
            data = list_of_dfs[0]
            data.rename(columns={'WEC Wise Report':'a', 'Unnamed: 1':'b', 'Unnamed: 2':'c', 'Unnamed: 3':'d',
            'Unnamed: 4':'e', 'Unnamed: 5':'f', 'Unnamed: 6':'g', 'Unnamed: 7':'h', 'Unnamed: 8':'i',
            'Unnamed: 9':'j', 'Unnamed: 10':'k', 'Unnamed: 11':'l', 'Unnamed: 12':'m',
            'Unnamed: 13':'n', 'Unnamed: 14':'o', 'Unnamed: 15':'p'},inplace=True)
            print(data)
            data.rename(columns={'a':data['a'].iloc[0], 'b':data['b'].iloc[0], 'c':data['c'].iloc[0],
                                 'd':data['d'].iloc[0],
                    'e':data['e'].iloc[0], 'f':data['f'].iloc[0], 'g':data['g'].iloc[0], 'h':data['h'].iloc[0], 'i':data['i'].iloc[0],
                    'j':data['j'].iloc[0], 'k':data['k'].iloc[0], 'l':data['l'].iloc[0], 'm':data['m'].iloc[0],
                    'n':data['n'].iloc[0], 'o':data['o'].iloc[0], 'p':data['p'].iloc[0]},inplace=True)
            data=data.iloc[1:,:]
            data.reset_index(drop=True,inplace=True)
            print('----------')
            print(data.info())
            data['GENERATION'] = data['GENERATION'].astype('int64')
            data['DATE'] = data['DATE'].astype('datetime64')
            #data['O.Hrs'] = data['O.Hrs'].astype('datetime64')
            #data['L.Hrs'] = data['L.Hrs'].astype('datetime64')
            data['MA '] = data['MA '].astype('float64')
            data['CF'] = data['CF'].astype('float64')
            data['GIA'] = data['GIA'].astype('float64')
            data['GA'] = data['GA'].astype('float64')
            sheet = data
            gen_data, bd_data = get_windworld_data(gen_data, bd_data, sheet)

        customers = {'D.J. Malpani': 'DJM', 'D J MALPANI': 'DJM', 'Giriraj Enterprises': 'GE', 'DJM': 'DJM',
                        'DJ Malpani Group': 'DJM',
                        'D J Malpani': 'DJM', 'NAKODA MACHINERY PVT. LTD.': 'NMPL', 'DJ Malpani': 'DJM',
                        'DJ Malpani - Palakkad': 'DJM',
                        'DJ Malpani - Sadla': 'DJM', 'DJ Malpani - Savarkundla': 'DJM', 'Giriraj Enterprises - Bagewadi': 'GE',
                        'IVY Ecoenergy India Private Ltd': 'IVY Ecoenergy India Private Ltd', 'Pravin Masalewale': 'PM',
                        'Hotel Golden Emerald': 'HGE', 'WESTERN PRECICAST PVT. LTD.': 'WPPL', 'Western Precicast Pvt Ltd':'WPPL',
                        'Jsons Foundry Pvt. Ltd': 'JSONF', 'J SONS GROUP': 'JSON', 'Amenity Developers And Builders':'ADB'}
        # print('gen data',gen_data)
      
        print("Success")
        if len(gen_data):
                gen_data['customer_name'] = [customers[x] for x in gen_data['customer_name']]
               
                #gen_data.to_csv('https://docs.google.com/spreadsheets/d/1fsgNCIHde801QANZ96XdihwUkWeYpLACTfI2T4rSpxg/edit#gid=0',header=False, mode='a',index=False)
                st.write('Done!')

        if len(bd_data):    
                bd_data['customer_name'] = [customers[x] for x in bd_data['customer_name']]
                #bd_data.to_csv('C:/Users/HP/Desktop/mastersheets/BreakdownMastersheet_DGR.csv',header=False, mode='a',index=False)
            
                st.write('Done!')


        print('DGR Consolidated Successfully')
        print('-----------------------------', end='\n\n')
       

consolidate()

