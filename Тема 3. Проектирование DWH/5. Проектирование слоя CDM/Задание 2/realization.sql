-- Создание ограничения PRIMARY KEY для поля id
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT pk_dm_settlement_report_id PRIMARY KEY (id);
