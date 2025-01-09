# ELT Data Pipeline with GCP and Airflow

Цей проект побудови конвеєру даних ELT (Extract, Load, Transform) для обробки 1 мільйона записів за допомогою Google Cloud Platform (GCP) та Apache Airflow. 

Конвеєр витягує дані з Google Cloud Storage (GCS), завантажує їх у BigQuery і трансформує для створення таблиць і подань для аналізу за країнами.

## Architecture
![Image alt](https://github.com/TurchinskiyD/global_health_elt_data_gcp/blob/main/health_data_gcp.png)

## Workflow
* Extract: Перевірити наявність файлу в GCS.
* Load: Завантажує необроблені дані CSV в таблицю BigQuery.
* Transform:
* Створення таблиць для конкретної країни в шарі перетворення.
* Створюйте подання звітів для кожної країни з відфільтрованою інформацією.

## Data Layers
* Staging Layer: Сирі дані з файлу CSV.
* Transform Layer: Очищені та перетвореня данних.
* Reporting Layer: Дані оптимізовані для аналізу та звітування.

## Airflow Pipeline
![Image alt](https://github.com/TurchinskiyD/global_health_elt_data_gcp/blob/main/Airflow_Pipeline.png)


## Looker Studio Report [clik here](https://lookerstudio.google.com/reporting/cc05e8f9-8cba-4de1-b3c0-031798483d90)
![Image alt](https://github.com/TurchinskiyD/global_health_elt_data_gcp/blob/main/dashbord_australia.png)


