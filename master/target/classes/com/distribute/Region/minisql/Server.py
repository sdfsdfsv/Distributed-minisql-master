from flask import Flask, request, jsonify
from Api import Api, Table, Attribute, TableRow, Condition

app = Flask(__name__)
api = Api()


@app.route('/create_table', methods=['POST'])
def create_table():
    try:
        res=None
        table_name = request.json['table_name']
        primary_key = request.json['primary_key']
        attributes = []
        for attr in request.json['attributes']:
            attributes.append(
                Attribute(attr['name'], attr['type'], attr['size'], attr['nullable']))
        table = Table(table_name, primary_key, attributes)
        res = api.createTable(table)

    except Exception:
        pass

    if res:
        return jsonify({'message': f'Table {table_name} created successfully'})
    else:
        return jsonify({'message': f'Table {table_name} creation failed'})


@app.route('/insert_row', methods=['POST'])
def insert_row():
    try:
        res=None
        table_name = request.json['table_name']
        values = []
        for value in request.json['values']:
            values.append(value)
        row = TableRow(values)
        res = api.insertRow(table_name, row)
        return jsonify({'message': 'Row inserted successfully'})

    except Exception:
        pass

    if res:
        return jsonify({'message': f'Table {table_name} inserted {values} successfully'})
    else:
        return jsonify({'message': f'Table {table_name} insertion failed'})


@app.route('/select', methods=['POST'])
def select():
    try:
        res=None
        table_name = request.json['table_name']
        columns = request.json['columns']
        conditions = []
        for cond in request.json['conditions']:
            conditions.append(
                Condition(cond['column'], cond['operator'], cond['value']))
        res = api.select(table_name, columns, conditions)

    except Exception:
        pass

    if res is not None:
        return jsonify({'rows': [str(r) for r in res]})
    else:
        return jsonify({'message': f'Table {table_name} selection failed'})


@app.route('/delete_row', methods=['POST'])
def delete_row():
    try:
        res=None
        table_name = request.json['table_name']
        conditions = []
        for cond in request.json['conditions']:
            conditions.append(
                Condition(cond['column'], cond['operator'], cond['value']))
        res = api.deleteRow(table_name, conditions)

    except Exception:
        pass

    if res is not None:
        return jsonify({'message': 'Row deleted successfully'})
    else:
        return jsonify({'message': 'Row deletion failed'})


@app.route('/drop_table', methods=['POST'])
def drop_table():
    try:
        res=None
        table_name = request.json['table_name']
        res = api.dropTable(table_name)
    except Exception:
        pass

    if res is not None:
        return jsonify({'message': f'Table {table_name} was dropped successfully'})
    else:
        return jsonify({'message': f'Table {table_name} failed to be dropped'})


@app.route('/store', methods=['POST'])
def store():
    api.store()
    return jsonify({'message': f'Table stored'})


@app.route('/init', methods=['POST'])
def init():
    api.init()
    return jsonify({'message': f'Table inited'})


if __name__ == '__main__':
    Api.init()
    app.run(debug=True, port=5000)
