
CREATE TABLE dds.dm_restaurants (
	id serial4 NOT NULL,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL
);

CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL,
	"_id" varchar NOT NULL,
	"name" varchar NOT NULL,
	CONSTRAINT courier_id UNIQUE (_id),
	CONSTRAINT couriers_pkey PRIMARY KEY (id)
);

CREATE TABLE dds.dm_deliveries (
	id serial4 NOT NULL,
	order_id varchar NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id varchar NOT NULL,
	address varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int2 NOT NULL,
	sum numeric(14, 2) NOT NULL DEFAULT 0,
	tip_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT fk_courier FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers("_id") ON DELETE CASCADE
);
