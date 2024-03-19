import pandas as pd
from connect import credentials
def insert_uf_and_municipio_csv(conn):
    try:
        # LENDO CSV COM PANDAS
        uf = pd.read_csv('estados.csv')
        municipio = pd.read_csv('municipios.csv')
        
        # CONECTANDO AO BANCO
        cursor = conn.cursor()
        # INSERINDO DADOS
        for index, row in uf.iterrows():
            cursor.execute(
                f'''
                INSERT INTO tb_uf(
	            co_uf, sg_uf, no_uf, nu_latitude, nu_longitude, co_usuario_inclusao, co_usuario_alteracao, st_registro_ativo, nu_versao)
	            VALUES ({row['codigo_uf']}, '{row['uf']}', '{row['nome']}', {row['longitude']}, {row['longitude']}, null, null, 'S', 1);
                '''
            )
            print(f'Inserindo {row["nome"]}...')
        for index, row in municipio.iterrows():
                cursor.execute(
                f'''
                INSERT INTO tb_municipio(
	            co_municipio, co_uf, no_municipio, nu_latitude, nu_longitude, co_usuario_inclusao, co_usuario_alteracao, st_registro_ativo, nu_versao)
	            VALUES ({row['codigo_ibge']}, '{row['codigo_uf']}', '{row['nome'].replace("'","")}', {row['longitude']}, {row['longitude']}, null, null, 'S', 1);
                '''
                )      
                print(f'Inserindo {row["nome"]}...')

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f'Erro: {e}')
        conn.rollback()
        cursor.close()
        conn.close()

