-- Создание ограничения уникальности
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_unique_constraint
UNIQUE (restaurant_id, settlement_date);
