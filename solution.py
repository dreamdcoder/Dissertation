from cassandra_data_mgmt import data


def get_solution(node_name, anomaly_type):
    d = data()
    df = d.read_data(node_name, 'knowledge_base')
    top_solution = df[df.success_rate == df.success_rate.max()]
    d.close_session()
    #print(top_solution.solution.to_string(index=False))
    return top_solution.solution.to_string(index=False)


if __name__ == '__main__':
    get_solution('leaf1', anomaly_type='BGP Anomaly')
