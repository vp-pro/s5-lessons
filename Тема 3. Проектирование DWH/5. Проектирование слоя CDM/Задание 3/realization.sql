alter table de.cdm.dm_settlement_report
    add constraint dm_settlement_report_date_check
        check (settlement_date between '2022-01-01' and '2500-01-01')