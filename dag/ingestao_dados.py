#################################################################
# Lista os arquivos no Google Cloud Storage (GCS)
# Carrega esses arquivos para o BigQuery, criando uma tabela para cada um

#################################################################


from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago, datetime  #

default_args = {
    'start_date': days_ago(1),
}

with DAG(
	dag_id='dag_gcs_para_bigquery',
	default_args=default_args,
    schedule_interval=None,
	catchup=False,
	tags=['gcp', 'bigquery', 'gcs'],
) as dag:

	start = DummyOperator(task_id='start')

	project_id = "marketplace-challenge-456003"
	dataset_id = "teste_dax"
	tabelas = ["associado",
				"categoria",
				"cliente",
				"filial",
				"item",
				"item_venda",
				"municipio",
				"subcategoria",
				"uf",
				"venda"]  # substitua pelos nomes reais


	tarefa_anterior = start  # Para encadear as tarefas

	for tabela in tabelas:
		carregar_tabela = GCSToBigQueryOperator(
			task_id=f"carregar_{tabela}",
			bucket='marketplace-bucket-jess',
			source_objects=[f"{tabela}.csv"],
			destination_project_dataset_table=f"{project_id}.{dataset_id}.{tabela}",
			source_format='CSV',
			skip_leading_rows=1,
			write_disposition='WRITE_TRUNCATE',
			field_delimiter=',',
			autodetect=True,
			gcp_conn_id='google_cloud_default',
		)

		tarefa_anterior >> carregar_tabela  # Encadeia a tarefa
		tarefa_anterior = carregar_tabela  # Atualiza o encadeamento

	fim = DummyOperator(task_id='fim')
	tarefa_anterior >> fim