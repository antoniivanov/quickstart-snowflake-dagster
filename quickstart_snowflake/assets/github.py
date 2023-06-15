from dagster import AssetKey, SourceAsset, asset
from dagster_snowflake import SnowflakeResource

github_asset = SourceAsset(key=AssetKey("github_asset"))


@asset(non_argument_deps={"github_asset"})
def dimension_date(snowflake: SnowflakeResource) -> None:
    with snowflake.get_connection() as conn:
        conn.cursor().execute("DROP TABLE IF EXISTS datawarehouse.dim_date")
        conn.cursor().execute(
            """
            CREATE TABLE datawarehouse.dim_date AS
            SELECT
                TO_VARCHAR(date, 'YYYY-MM-DD') AS date_id,
                date,
                EXTRACT(YEAR FROM date) AS year,
                EXTRACT(MONTH FROM date) AS month,
                EXTRACT(DAY FROM date) AS day
            FROM
            (
                SELECT DATEADD('YEAR', -1, CURRENT_DATE()) + seq4() AS date
                FROM TABLE(GENERATOR(ROWCOUNT => 2*365))
            )
            WHERE date <= CURRENT_DATE()
            ORDER BY date_id
            """
        )


@asset(non_argument_deps={"github_asset"})
def dimension_component(snowflake: SnowflakeResource) -> None:
    with snowflake.get_connection() as conn:
        conn.cursor().execute("DROP TABLE IF EXISTS datawarehouse.dim_component")
        conn.cursor().execute(
            """
            CREATE TABLE datawarehouse.dim_component AS
            SELECT
                SUBSTR(TITLE, 1, CHARINDEX(':', TITLE, 1) - 1 ) AS component_id,
                SUBSTR(TITLE, 1, CHARINDEX(':', TITLE, 1) - 1 ) AS component_name
            FROM TEST.TEST.PULL_REQUESTS
            GROUP BY component_name
            """
        )


@asset(non_argument_deps={"dimension_date", "dimension_component"})
def fact_pr_merged(snowflake: SnowflakeResource) -> None:
    with snowflake.get_connection() as conn:
        conn.cursor().execute("DROP TABLE IF EXISTS datawarehouse.fact_pr_merged")
        conn.cursor().execute(
            """
            CREATE TABLE datawarehouse.fact_pr_merged AS
            SELECT
                ROW_NUMBER() OVER (ORDER BY CREATED_AT) AS pr_merged_id,
                D.date_id,
                C.component_id,
                TIMESTAMPDIFF(HOUR, CREATED_AT, MERGED_AT) AS merge_time,
                (SELECT COUNT(*) FROM TEST.TEST.REVIEW_COMMENTS WHERE PULL_REQUEST_URL = PULL_REQUESTS.URL) AS comment_count
            FROM TEST.TEST.PULL_REQUESTS AS PULL_REQUESTS
            JOIN dim_date AS D ON TO_DATE(DATE_TRUNC('day', PULL_REQUESTS.CREATED_AT)) = D.date
            JOIN dim_component AS C ON STARTSWITH(PULL_REQUESTS.TITLE, C.component_id) = true
            WHERE MERGED_AT is not NULL 
            """
        )