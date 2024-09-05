-- Create table
create or replace table completed_orders(
    order_id int,
    product varchar(20),
    quantity int,
    order_status varchar(30),
    order_date date
);

-- Select table
select * from completed_orders;

-- Schedule a task to ingest data in completed_orders from snowpipe source table
CREATE OR REPLACE TASK target_table_ingestion
WAREHOUSE = COMPUTE_WAREHOUSE
SCHEDULE = 'USING CRON */2 * * * * UTC' -- every 2 minutes
AS
INSERT INTO completed_orders SELECT * FROM orders_data_lz where order_status = 'Completed';;

--- By default, a new task is created in a suspended state. You need to resume it to start its execution as per the defined schedule.

ALTER TASK target_table_ingestion RESUME;

--- To suspend a task
ALTER TASK target_table_ingestion SUSPEND;

--- check the history of task
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'target_table_ingestion')) ORDER BY SCHEDULED_TIME;

--- drop a task

DROP TASK target_table_ingestion;

--- Example of chaining tasks

CREATE OR REPLACE TASK next_task
  WAREHOUSE = COMPUTE_WAREHOUSE
  AFTER target_table_ingestion
AS
  DELETE FROM completed_orders WHERE order_date < CURRENT_DATE();