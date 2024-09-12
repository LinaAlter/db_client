import psycopg2
from psycopg2.sql import SQL, Identifier

def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE clients_phones;
        DROP TABLE clients_info;
        """);
        cur.execute("""CREATE TABLE IF NOT EXISTS clients_info(
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(60) NOT NULL,
                    last_name VARCHAR(60) NOT NULL,
                    email VARCHAR(60) NOT NULL
                    )
                    """);
        cur.execute(""" CREATE TABLE IF NOT EXISTS clients_phones(
                    client_id INTEGER NOT NULL REFERENCES clients_info(id),
                    phone_number VARCHAR(12) 
                    )
                    """);
        print('База данных успешно создана.')

def add_client(conn, id, first_name, last_name, email, phone_number=None):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO clients_info
                    VALUES(%s, %s, %s, %s)
                    RETURNING id, first_name, last_name, email;""", (id, first_name, last_name, email)
                   );
        print('Добавлен новый клиент: ', cur.fetchone())
                   
def add_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients_phones(client_id, phone_number)
            VALUES(%s, %s)
            RETURNING client_id, phone_number;
            """, (client_id, phone_number));
        print(f'Добавлен новый номер телефона {phone_number} для клиента с идентификатором {client_id}')

def change_client(conn, id, first_name=None, last_name=None, email=None, phones=None):
    with conn.cursor() as cur:
        arg_list = {'first_name': first_name, 'last_name': last_name, 'email': email}
        for key, arg in arg_list.items():
            if arg:
                cur.execute(SQL('UPDATE clients_info SET {}=%s WHERE id = %s').format(Identifier(key)),(arg, id))
        cur.execute("""
            SELECT * FROM clients_info
            WHERE id = %s;
            """, id)
        print('Данные изменены.')
        print(cur.fetchall())



def delete_phone(conn, client_id, phone_number):
    with conn.cursor() as cur:
        cur.execute("""DELETE FROM clients_phones
                    WHERE client_id = %s AND phone_number = %s
                    RETURNING client_id, phone_number;
                    """, (client_id, phone_number))
        print('Номер телефона клиента удален.')        
        return cur.fetchone()


def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""DELETE FROM clients_phones
                    WHERE client_id =%s
                    RETURNING client_id;
                    """, (client_id,))
        cur.execute("""DELETE FROM clients_info
                  WHERE id =%s
                  RETURNING id;
                  """, (client_id,))
        print('Данные клиента удалены.')
        return cur.fetchone()
        
    
def find_client(conn, first_name=None, last_name=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        cur.execute("""SELECT * FROM clients_info ci
                    LEFT JOIN clients_phones cph ON ci.id = cph.client_id
                    WHERE (first_name = %(first_name)s OR %(first_name)s IS NULL)
                    OR (last_name = %(last_name)s OR %(last_name)s IS NULL)
                    OR (email = %(email)s OR %(email)s IS NULL)
                    OR (phone_number = %(phone_number)s OR %(phone_number)s IS NULL);
                    """, {"first_name": first_name, "last_name": last_name, "email": email, "phone_number": phone_number})
        print('Найден клиент:\n', cur.fetchone())


with psycopg2.connect(database="clients_db", user="postgres", password="1358") as conn:
    create_db(conn)
    add_client(conn, 1,'Joe', 'Cocker', 'someemail1@test.com')
    add_client(conn, 2, 'Amy', 'Strong', 'someemail2@test.com')
    add_phone(conn, 1, 89831111111)
    add_phone(conn, 1, 89831111112)
    change_client(conn, '1', 'James', 'Button', 'somenew@test.com')
    delete_phone(conn, 1, '89831111111')
    delete_client(conn, 1)
    find_client(conn, 'Strong')
conn.close()       