-- Ограничение CHECK для поля orders_count
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_orders_count_check
CHECK (orders_count >= 0);

-- Ограничение CHECK для поля orders_total_sum
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_orders_total_sum_check
CHECK (orders_total_sum >= 0);

-- Ограничение CHECK для поля orders_bonus_payment_sum
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check
CHECK (orders_bonus_payment_sum >= 0);

-- Ограничение CHECK для поля orders_bonus_granted_sum
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check
CHECK (orders_bonus_granted_sum >= 0);

-- Ограничение CHECK для поля order_processing_fee
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_order_processing_fee_check
CHECK (order_processing_fee >= 0);

-- Ограничение CHECK для поля restaurant_reward_sum
ALTER TABLE cdm.dm_settlement_report
ADD CONSTRAINT dm_settlement_report_restaurant_reward_sum_check
CHECK (restaurant_reward_sum >= 0);
