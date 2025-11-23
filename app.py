import pymysql
from pymysql.cursors import DictCursor
from flask import Flask, jsonify, request
from flask_cors import CORS
import random

app = Flask(__name__)
CORS(app)

# ==========================================
# 数据库配置 (已更新密码)
# ==========================================
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '123456',  # <--- 已在这里自动添加你的密码
    'db': 'data_lineage_system',
    'charset': 'utf8mb4',
    'cursorclass': DictCursor
}

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

# ==========================================
# API 1: 获取左侧表格数据
# ==========================================
@app.route('/api/nodes', methods=['GET'])
def get_nodes():
    search_query = request.args.get('q', '')
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                SELECT node_id, proc_name, table_name, column_name, node_type, parent_ids_str, logic_types
                FROM v_lineage_table_view
                WHERE table_name LIKE %s OR column_name LIKE %s OR proc_name LIKE %s
                ORDER BY node_id DESC
                LIMIT 100
            """
            like_query = f"%{search_query}%"
            cursor.execute(sql, (like_query, like_query, like_query))
            result = cursor.fetchall()
            return jsonify(result)
    except Exception as e:
        print(f"Error fetching nodes: {e}")
        return jsonify([])
    finally:
        conn.close()

# ==========================================
# API 2: 核心功能 - 递归获取血缘树
# ==========================================
@app.route('/api/lineage/<int:node_id>', methods=['GET'])
def get_lineage_tree(node_id):
    nodes = []
    edges = []
    visited_nodes = set()
    added_node_ids = set()

    def fetch_upstream_recursive(current_id):
        if current_id in visited_nodes:
            return
        visited_nodes.add(current_id)

        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                # 1. 获取节点详情
                cursor.execute("SELECT * FROM lineage_nodes WHERE node_id = %s", (current_id,))
                node_info = cursor.fetchone()
                if not node_info:
                    return

                s_id = str(node_info['node_id'])
                if s_id not in added_node_ids:
                    nodes.append({
                        "id": s_id,
                        "label": f"{node_info['table_name']}\n{node_info['column_name']}",
                        "node_type": node_info['node_type'],
                        "full_info": node_info
                    })
                    added_node_ids.add(s_id)

                # 2. 获取上游
                sql_edges = """
                    SELECT upstream_node_id, transform_type 
                    FROM lineage_edges 
                    WHERE downstream_node_id = %s
                """
                cursor.execute(sql_edges, (current_id,))
                parents = cursor.fetchall()

                for parent in parents:
                    p_id = parent['upstream_node_id']
                    edges.append({
                        "source": str(p_id),
                        "target": str(current_id),
                        "label": parent['transform_type'] or "Direct"
                    })
                    fetch_upstream_recursive(p_id)
        finally:
            conn.close()

    fetch_upstream_recursive(node_id)
    
    return jsonify({ "nodes": nodes, "edges": edges })

# ==========================================
# API 3: SQL 解析 (模拟)
# ==========================================
@app.route('/api/parse', methods=['POST'])
def parse_sql():
    data = request.json
    sql_text = data.get('sql', '')
    
    if not sql_text:
        return jsonify({'status': 'error', 'message': 'SQL content is empty'})

    conn = get_db_connection()
    new_nodes_count = 0
    
    try:
        with conn.cursor() as cursor:
            # 模拟解析逻辑
            suffix = str(random.randint(1000, 9999))
            proc_name = f"Auto_Parsed_SP_{suffix}"
            
            cursor.execute("INSERT INTO lineage_nodes (proc_name, table_name, column_name, node_type) VALUES (%s, %s, %s, %s)", 
                           (proc_name, 'source_table', f'field_a_{suffix}', 'SOURCE'))
            node_a_id = cursor.lastrowid
            
            cursor.execute("INSERT INTO lineage_nodes (proc_name, table_name, column_name, node_type) VALUES (%s, %s, %s, %s)", 
                           (proc_name, 'source_table', f'field_b_{suffix}', 'SOURCE'))
            node_b_id = cursor.lastrowid
            
            cursor.execute("INSERT INTO lineage_nodes (proc_name, table_name, column_name, node_type) VALUES (%s, %s, %s, %s)", 
                           (proc_name, 'target_table', f'result_c_{suffix}', 'TARGET'))
            node_c_id = cursor.lastrowid
            
            cursor.execute("INSERT INTO lineage_edges (downstream_node_id, upstream_node_id, transform_type) VALUES (%s, %s, %s)",
                           (node_c_id, node_a_id, 'COALESCE'))
            cursor.execute("INSERT INTO lineage_edges (downstream_node_id, upstream_node_id, transform_type) VALUES (%s, %s, %s)",
                           (node_c_id, node_b_id, 'COALESCE'))
            
            new_nodes_count = 3
            conn.commit()

    except Exception as e:
        conn.rollback()
        return jsonify({'status': 'error', 'message': str(e)})
    finally:
        conn.close()

    return jsonify({'status': 'success', 'nodes_count': new_nodes_count})

if __name__ == '__main__':
    # 【重要修改】use_reloader=False 用于解决 WinError 10038
    app.run(debug=True, port=5000, use_reloader=False)