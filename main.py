import psycopg2
from connect import credentials
from insert import insert_uf_and_municipio_csv
import mysql.connector

options = {
    'pg': 'PostgreSQL',
    'mysql': 'MySQL',
    'sqlserver': 'SQL Server'
}

def main():
    print('Escolha o SGBD que deseja conectar:')
    for key, value in options.items():
        print(f'{key} - {value}')
    choice = input('Digite a opção desejada:')
    print(f'Você escolheu: {options[choice]}')

    
    if(choice == 'pg'):
        print('Conectando ao PostgreSQL...')
        conn = psycopg2.connect(**credentials)
    if(choice == 'mysql'):
        print('Conectando ao MySQL...')
        conn = mysql.connector.connect(**credentials)
    
    insert_uf_and_municipio_csv(conn)


if __name__ == '__main__':
    main()