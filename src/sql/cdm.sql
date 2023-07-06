
CREATE TABLE cdm.dm_courier_ledger (
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	rate_avg numeric(14, 2) NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0
);
