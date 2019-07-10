#_38: If the waiting time is less than 7 seconds, the simulator will decide automatically; Otherwise, mark the reply as noreply and push the transaction to the database
#_38: Banker has 3 buttons: yes: accept; no: reject; no reply: reject to decide manually and update the database automatically.
#Make new botton to hold the index and phone number
#Make both left and right live, triggered by the
#Two images.Left is live, right one is not.
import dash
import dash_core_components as dcc
import dash_html_components as html
#import dash_table_experiments as dt
from dash.dependencies import Input, Output, State
import psycopg2
import pandas as pd
import plotly.graph_objs as go
import subprocess
from datetime import datetime
import time
import numpy as np


#initialize variables to plot
latency = [0]  #latency
transactions = [0] #transactions
times = [ 0 ]
run= False
#time = [ int( time.time()*1000 ) ]

transaction_index= "00000"
transaction_number="775 666 3471"
transaction_timeprocessed= "122222222222"   #The combination of index and timeprocessed is the handle of the transaction to update it later.
time_updated= -int( time.time() )
period_update=5

#establish initial connection to postgres db to create transactions table
connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
cursor = connection.cursor()
cursor.execute('DELETE FROM transactions;')
connection.commit()
cursor.close()
connection.close()

#establish initial connection to postgres db to create transactions table
app = dash.Dash()

#div containing html and dash elements that are displayed on the page
app.layout = html.Div(children=[
    #### 1
    html.Div(  #division 1, the head
        html.H2('FRAND ALERT: a real-time fraud detection system with feedback!'),
        className = 'banner'
    ),
    #### 2
    html.Div(children = [  #division 2, the buttons
        html.Button(
            id = 'startButton',
            children = 'Start'
        ),

        html.Button(
            id = 'secondButton',
            children = 'Second'
        ),

        html.Button(
            id = 'thirdButton',
            children = 'Third'
        ),

        html.Button(
            id = 'stopButton',
            children = 'Stop'
        )
    ]),

    html.Div(children = [  #division 2, the buttons
        html.Div(
            id = 'startLabel'
        ),

        html.Div(
            id= 'secondLabel'
        ),

        html.Div(
            id= 'thirdLabel'
        ),


        html.Div(
            id = 'stopLabel'
        )
    ]),

    #### 3.
    html.Div([  #division 3, the fraud information
        html.Div([
            html.H3("Customer ID: Phone number")
        ], className='Title'),
        html.Div(
            id= 'informationLabel'
        ),
        html.Button(
            id = 'callButton',
            children = 'Call'
        ),
        dcc.Interval(id='updateTable', interval=1000, n_intervals=0)   #update the image
    ], className='row wind-speed-row'),
    
    #The accept and reject button
    html.Div(children = [  #division 2, the buttons
        html.Button(
            id = 'acceptButton',
            children = 'Yes: Accept'
        ),

        html.Button(
            id = 'rejectButton',
            children = 'No: Reject'
        ),
        
        html.Button(
            id = 'noanswerButton',
            children = 'No answer: Reject'
        ),
        
        html.Div(
            id = 'acceptLabel'
        ),

        html.Div(
            id= 'rejectLabel'
        ),
        
        html.Div(
            id = 'noanswerLabel',
        ),      
    ]),

    #### 4.
    html.Div([

        html.Div([
            html.H3('Transactions per second'),
            dcc.Graph(id='fig_transaction')
        ], style={'width': '48%', 'display': 'inline-block'}),

        html.Div([
            html.H3('Latency'),
            dcc.Graph(id='fig_latency')
        ], style={'width': '48%', 'align': 'right', 'display': 'inline-block'} )
    ]),
    #division 5, the buttons
    html.Div(children = [
        html.Button(
            id = 'tButton',
            children = '#Transactions'
        ),
        
        html.Button(
            id = 'tpButton',
            children = '#True Pos.'
        ),

        html.Button(
            id = 'fpButton',
            children = '#False Pos.'
        ),

        html.Div(
            id = 'tLabel'
        ),
        
        html.Div(
            id= 'tpLabel'
        ),

        html.Div(
            id= 'fpLabel'
        )
    ])

    #dt.DataTable( #division 3, the table
    #id = 'trades',
    #rows=[{}],
    #row_selectable=False,
    #filterable=False,
    #sortable=False,
    #selected_row_indices=[]
    #)
])

#function that initiates producer script and spark submit upon button click
#see run.sh for information on how to start producer and submit spark job
@app.callback(
    Output('startLabel', 'children'),  #
    [Input('startButton','n_clicks')]
)
def startTrading(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        subprocess.Popen('bash run_simulator.sh', shell=True)
        time.sleep(1)
        subprocess.Popen('bash run_random.sh', shell=True)  #Attach & at the end of the commond to make sure run two producers at the same time
        subprocess.Popen('bash run_random_2.sh', shell=True)
        global run
        times.clear()
        times.append(0)
        latency.clear()
        latency.append(0)
        transactions.clear()
        transactions.append(0)
        run= False
        return 'Started!'

@app.callback(
    Output('secondLabel', 'children'),  #
    [Input('secondButton','n_clicks')]
)
def secondTrading(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        subprocess.Popen('bash run_random_3.sh', shell=True)
        subprocess.Popen('bash run_random_4.sh', shell=True)
        return 'Second started!'

@app.callback(
    Output('thirdLabel', 'children'),  #
    [Input('thirdButton','n_clicks')]
)
def thirdTrading(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        subprocess.Popen('bash run_random_5.sh', shell=True)
        subprocess.Popen('bash run_random_6.sh', shell=True)
        return 'Third started!'



#function that stops producer script and spark job upon button click
#see stop.sh for information on how to stop producer and spark submit job
@app.callback(
    Output('stopLabel', 'children'),
    [Input('stopButton', 'n_clicks')]
)
def stopTrading(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        global run
        subprocess.Popen('bash stop_random_5.sh', shell=True)
        subprocess.Popen('bash stop_random.sh', shell=True)
        subprocess.Popen('bash stop_random_2.sh', shell=True)
        subprocess.Popen('bash stop_random_3.sh', shell=True)
        subprocess.Popen('bash stop_random_4.sh', shell=True)        
        subprocess.Popen('bash stop_random_6.sh', shell=True)
        subprocess.Popen('bash stop_simulator.sh', shell=True)
        
        time.sleep(5)
        run= False
        return 'Stopped.'

@app.callback(
    Output('informationLabel', 'children'),
    [Input('updateTable','n_intervals')]
)
def updateInformation(n):
    connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
    time_now=str( int(time.time()*1000) )
    #Only get transaction whose time_processed is in the past.
    data = pd.read_sql_query("SELECT index, phonenumber, timeprocessed FROM transactions WHERE prediction='yes' AND reply='noreply' AND timeprocessed<= '"+ time_now + "' ORDER BY timeprocessed DESC LIMIT 1 ;", connection)  #undecided transactions
    if len(data)>0:
        global time_updated, transaction_index, transaction_timeprocessed, transaction_number
        if time_updated< 0:  #first update
            time_updated=int(time.time() )
            data = data.to_dict('records')[0]	  #to_dict return the list of dictionary[ {"count": #}, {}
            transaction_index= data["index"]
            transaction_number= data["phonenumber"]
            transaction_timeprocessed= data["timeprocessed"]   #The combination is the handle of the transaction to update it later.
            return data["index"]+":       "+ data["phonenumber"]
        elif time.time()- time_updated>= period_update:
            time_updated=int(time.time() )
            data = data.to_dict('records')[0]	  #to_dict return the list of dictionary[ {"count": #}, {}
            transaction_index= data["index"]
            transaction_number= data["phonenumber"]
            transaction_timeprocessed= data["timeprocessed"]   #The combination is the handle of the transaction to update it later.
            return data["index"]+":       "+ data["phonenumber"]
        else:
            return transaction_index+":       "+ transaction_number
        
        
@app.callback(
    Output('acceptLabel', 'children'), 
    [Input('acceptButton','n_clicks')]
)
def acceptTransaction(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
        cursor = connection.cursor()
        cursor.execute("""UPDATE transactions SET reply= %s WHERE index= %s AND timeprocessed= %s""", ("no", transaction_index, transaction_timeprocessed) )
        connection.commit()
        cursor.close()
        connection.close()
        return 'The transaction is accepted and updated in DB!'

@app.callback(
    Output('rejectLabel', 'children'), 
    [Input('rejectButton','n_clicks')]
)
def rejectTransaction(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
        cursor = connection.cursor()
        cursor.execute("""UPDATE transactions SET reply= %s WHERE index= %s AND timeprocessed= %s""", ("yes", transaction_index, transaction_timeprocessed) )
        connection.commit()
        cursor.close()
        connection.close()
        return 'The transaction is rejected and updated in DB!'
        
@app.callback(
    Output('noanswerLabel', 'children'), 
    [Input('noanswerButton','n_clicks')]
)
def rejectTransaction(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
        cursor = connection.cursor()
        cursor.execute("""UPDATE transactions SET reply= %s WHERE index= %s AND timeprocessed= %s""", ("noreply", transaction_index, transaction_timeprocessed) )
        connection.commit()
        cursor.close()
        connection.close()
        return 'The transaction is rejected and updated in DB!'


#displays the 20 most recent trades - updates every 0.5s
#@app.callback(
#Output('trades', 'rows'),
#[Input('updateTable','n_intervals')]
#)
#def updateTable(n):   #update table
#connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
#data = pd.read_sql_query('SELECT index, phonenumber, amount, prediction FROM transactions ORDER BY timeprocessed DESC LIMIT 5;', connection)
#return data.to_dict('records')

#displays the current value of the portfolio every 0.5s
@app.callback(
    Output('fig_transaction', 'figure'),
    [Input('updateTable','n_intervals')]
)
def updateGraph(n):
    time_now= int( 1000* time.time() )
    connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
    data = pd.read_sql_query("SELECT COUNT(*), SUM(latency::INTEGER) FROM transactions WHERE timeprocessed > '"+ str(time_now-2000)+ "' AND timeprocessed < '"+ str(time_now -1000) +"'  AND prediction= 'no';", connection)  #There are two classes: prediction='no' is the majority.
    data = data.to_dict('records')[0]	  #to_dict return the list of dictionary[ {"count": #}, {}

    #maintain time series of stock value and cash value

    global run
    run= ( run or data["count"]>100 )

    if run:
        times.append(times[len(times) - 1] + 1)
        
        if data["count"]>100:
            transactions.append( data["count"] )
            latency.append( data["sum"]/data["count"] )
        else:
            transactions.append(0)
            latency.append(0)
            
        print(times,  latency, transactions);
        print()


    trace0 = go.Scatter(
        x = times,
        y = transactions,
        mode = 'lines',
        name = 'Stock',
        line = dict(
            color = ('rgb(66, 196, 247)')
        )
    )
    
    layout = dict(
        xaxis = dict(title = 'Elapsed Time (s)'),
        yaxis = dict(title = '#transactions')
    )

    #keep track of elapsed seconds


    return go.Figure(data = [trace0], layout = layout)


@app.callback(
    Output('fig_latency', 'figure'),
    [Input('updateTable','n_intervals')]
)
def updateGraph(n):

    trace0 = go.Scatter(
        x = times,
        y = latency,
        mode = 'lines',
        name = 'Stock',
        line = dict(
            color = ('rgb(66, 196, 247)')
        )
    )

    layout = dict(
        xaxis = dict(title = 'Elapsed Time (s)'),
        yaxis = dict(title = 'latency (ms)')
    )

    return go.Figure(data = [trace0], layout = layout)
    
@app.callback(
    Output('tLabel', 'children'),
    [Input('tButton','n_clicks')]
)
def searchT(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
        data = pd.read_sql_query("SELECT COUNT(*) FROM transactions;", connection)
        if len(data)>0:
            data = data.to_dict('records')[0]	  #to_dict return the list of dictionary[ {"count": #}, {}
            return data["count"]

@app.callback(
    Output('tpLabel', 'children'),
    [Input('tpButton','n_clicks')]
)
def searchTP(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
        data = pd.read_sql_query( "SELECT COUNT(*) FROM transactions WHERE prediction='yes' AND reply='yes';", connection )
        if len(data)>0:
            data = data.to_dict('records')[0]	  #to_dict return the list of dictionary[ {"count": #}, {}
            return data["count"]
        
@app.callback(
    Output('fpLabel', 'children'),
    [Input('fpButton','n_clicks')]
)
def searchFP(n_clicks):
    if n_clicks is not None and n_clicks > 0:
        connection = psycopg2.connect(host = '10.0.0.4', port='5431', database = 'test_db', user = 'db_select', password = 'password')
        data = pd.read_sql_query( "SELECT COUNT(*) FROM transactions WHERE prediction='yes' AND reply='no';", connection )
        if len(data)>0:
            data = data.to_dict('records')[0]	  #to_dict return the list of dictionary[ {"count": #}, {}
            return data["count"]

#css sheets used to style graphs, headers, etc.
external_css = ["https://cdnjs.cloudflare.com/ajax/libs/skeleton/2.0.4/skeleton.min.css",
                "https://cdn.rawgit.com/plotly/dash-app-stylesheets/737dc4ab11f7a1a8d6b5645d26f69133d97062ae/dash-wind-streaming.css",
                "https://fonts.googleapis.com/css?family=Raleway:400,400i,700,700i",
                "https://fonts.googleapis.com/css?family=Product+Sans:400,400i,700,700i"]

#for css in external_css:
#    app.css.append_css({"external_url": css})

if __name__ == '__main__':
    app.run_server(debug=True, host = '0.0.0.0')
