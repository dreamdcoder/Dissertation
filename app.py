import flask
from flask import Flask,render_template
from cassandra_data_mgmt import data
import pandas as pd


REST_API_URL= 'https://api.powerbi.com/beta/c8eca3ca-1276-46d5-9d9d-a0f2a028920f/datasets/b95e3666-7d39-4faf-b49a-aad8879f1021/rows?key=GMq1fua6vtBWckini28U0Lw06se3xhcvV4LgxZ1jJW%2F7jcki33AoqGsJXDZDXce%2BRbSacaSBi49acMNkDmNHNA%3D%3D'

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_data')
def get_data():
  d = data()
  df = d.read_data('leaf1')
  cols = ['key', 'time', 'active-routes-count', 'backup_routes_count', 'deleted_routes_count', 'paths_count',
          'performance_stat_global_config_items_processed',
          'performance_stat_vrf_inbound_update_messages',
          'protocol_route_memory', 'total_neighbors_count',
          'vrf_path_count', 'vrf_update_messages_received']
  df_new= df[['protocol_route_memory','time']]
  print(df_new)
  #labels = ["Africa", "Asia", "Europe", "Latin America", "North America"]
  #df = [5578,5267,734,784,433]
  #return flask.jsonify({'payload':json.dumps({'data':data, 'labels':labels})})


if __name__ == "__main__":
   app.run(debug=True)
