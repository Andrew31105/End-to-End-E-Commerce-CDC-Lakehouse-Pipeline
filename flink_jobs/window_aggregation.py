import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# 1. Nạp .env linh hoạt (bỏ qua nếu chạy trong Docker không có python-dotenv)
load_dotenv_flag = os.getenv("LOAD_DOTENV", "True").lower() == "true"
if load_dotenv_flag:
    try:
        from dotenv import load_dotenv as _load_dotenv
        _load_dotenv()
    except ImportError:
        pass  # python-dotenv không có (Docker container) → dùng env vars trực tiếp

def main():
    # 2. Khởi tạo Flink Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000) # Checkpoint mỗi 10s
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 3. Đọc & inject biến môi trường vào file SQL
    sql_file_path = os.path.join(os.path.dirname(__file__), "window_aggregates.sql")
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    rendered_sql = sql_content.format(
        KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        KAFKA_TOPIC=os.getenv("KAFKA_TOPIC", "pg.public.orders"),
        CLICKHOUSE_HOST=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        CLICKHOUSE_PORT=os.getenv("CLICKHOUSE_PORT", "8123"),
        CLICKHOUSE_DB=os.getenv("CLICKHOUSE_DB", "ecommerce"),
        CLICKHOUSE_USER=os.getenv("CLICKHOUSE_USER", "admin"),
        CLICKHOUSE_PASSWORD=os.getenv("CLICKHOUSE_PASSWORD", "admin123")
    )

    # 4. Tách các câu SQL bằng dấu chấm phẩy và thực thi
    #    Lưu ý: loại bỏ dòng comment (-- ...) TRƯỚC khi split(';') — vì ';' nằm trong
    #    comment sẽ làm câu lệnh bị cắt cụt (SqlParserEOFException).
    sql_without_comments = "\n".join(
        ln for ln in rendered_sql.splitlines() if not ln.strip().startswith("--")
    )
    sql_statements = [stmt.strip() for stmt in sql_without_comments.split(";") if stmt.strip()]
    
    statement_set = t_env.create_statement_set()
    
    for stmt in sql_statements:
        if stmt.startswith("INSERT"):
            statement_set.add_insert_sql(stmt)
        else:
            t_env.execute_sql(stmt)

    # 5. Trigger Flink Execution
    job_client = statement_set.execute()
    print(f" Flink Tumbling Window Job Submitted Successfully! Job ID: {job_client.get_job_client().get_job_id()}")

if __name__ == "__main__":
    main()