# Антифрод: потоковый скоринг и переобучение модели в Airflow

Сервис скоринга фродовых транзакций (Kafka → CatBoost → PostgreSQL) и два DAG в Apache Airflow: поставка нового размеченного месяца и скользящее переобучение модели. Основа проекта: код семинара 7, препроцессинг изменён (см. ниже).

## Как поднять проект
1. Скопируйте `.env.example` в `.env`: `cp .env.example .env`
2. В `.env` в переменной `TRAIN_DATA_CSV` укажите абсолютный путь к `train.csv` (датасет: https://www.kaggle.com/competitions/teta-ml-1-2025/data).
3. Запустите: `docker compose up --build`. Первый запуск занимает 15-30 минут (скачивание образов и загрузка витрины).

## Где что открыть
- Airflow UI: http://localhost:8081 (логин `admin`, пароль `admin`)
- Kafka UI: http://localhost:8080
- PostgreSQL: `localhost:5432`, база `fraud_db`

## Как запустить DAG
В Airflow откройте DAG `retrain_catboost` и нажмите ▶ Trigger DAG. Цепочка задач: `extract_month_batch → slide_training_window → time_train_test_split → preprocess_dataset → fit_catboost → publish_model`. DAG `supply_labeled_batch` дописывает в витрину новый месяц. Оба DAG идут по расписанию раз в 5 минут, catchup выключен.

## Параметры
- Глубина истории витрины: 3 года
- Размер батча: 100 000 строк
- Окно обучения: 3 месяца (train: два более ранних месяца, test: самый свежий; разбиение по времени)

## Как убедиться, что скорится опубликованной версией модели
1. После успешного прогона DAG в каталоге моделей появляется новый файл:
   `docker compose exec airflow ls -la /opt/airflow/models`
2. Скоринг подхватывает модель из `latest.txt` без перезапуска контейнера.
3. Версия модели видна в таблице `scores` (колонка `model_version`):
   `docker compose exec postgres psql -U fraud_user -d fraud_db`
   ```sql
   SELECT transaction_id, score, fraud_flag, model_version, created_at
   FROM scores ORDER BY created_at DESC LIMIT 20;
   ```
   У новых строк `model_version` равна метке времени новой модели (до первого переобучения: `initial`).

## Мой препроцессинг (`fraud_detector/src/preprocessing.py`)
- Убраны признаки `year`, `month`, `day`: симулятор шлёт транзакции с текущей датой, а модель учится на старых месяцах, так что эти значения вне обучающего распределения.
- Час закодирован циклически: `hour_sin`, `hour_cos` (23:00 и 00:00 близки).
- Добавлен бинарный признак `night_transaction` (час ≥ 23 или ≤ 3).
- Сохранены `haversine` (расстояние клиент-продавец), `hour`, `minute`, `weekend`.
Одна и та же функция `run_preproc` используется и в DAG, и в онлайн-скоринге.

## Остановка
`docker compose down -v` удаляет контейнеры вместе с витриной, моделями и состоянием Airflow.
