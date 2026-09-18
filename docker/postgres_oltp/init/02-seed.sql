-- =======================================================
-- Seed data
-- =======================================================

SELECT setseed(0.56);

-- Fixed reference date for deterministic seed generation
WITH params as (
	SELECT TIMESTAMPTZ '2026-09-01 00:00:00+00' AS base_date
)
SELECT base_date
FROM params;

-- =======================================================
-- Customers
-- =======================================================

INSERT INTO customers (
	email, 
	first_name, 
	last_name,
	created_at,
	updated_at
)

WITH params as (
	SELECT TIMESTAMPTZ '2026-09-01 00:00:00+00' AS base_date
)

SELECT
	'customer' || n || '@example.com',
	'First' || n,
	'Last' || n,
	base_date - (n || ' days')::interval,
	base_date - (n || ' days')::interval
FROM
	generate_series(1, 100) as n
CROSS JOIN params;

-- =======================================================
-- Products
-- =======================================================

INSERT INTO products (
	name,
	category,
	price,
	created_at,
	updated_at
)

WITH params as (
	SELECT TIMESTAMPTZ '2026-09-01 00:00:00+00' AS base_date
),

categories as (
	SELECT
		ARRAY[
			'electronics',
			'book',
			'clothing',
			'home',
			'sports'
		] as value
)

SELECT
	'Product ' || n,
	value[((n - 1) % array_length(value, 1)) + 1],
	(10 + 1.25 * n)::numeric(12, 2),
	base_date - (n || ' days')::interval,
	base_date - (n || ' days')::interval
FROM
	generate_series(1, 50) as n
CROSS JOIN
	categories
CROSS JOIN
	params;


-- =======================================================
-- Generate orders and order items
-- =======================================================

CREATE TEMP TABLE seed_orders (
    seed_order_id INTEGER PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    payment_count INTEGER NOT NULL
);

CREATE TEMP TABLE seed_order_items (
	seed_order_id	integer not null,
	product_id		bigint not null,
	quantity		integer not null,
	unit_price		numeric(12, 2) not null
);

CREATE TEMP TABLE seed_order_map (
	seed_order_id	integer primary key,
	order_id		bigint not null
);

-- -------------------------------------------------------
-- seed_orders
-- -------------------------------------------------------

INSERT INTO seed_orders (
    seed_order_id,
    customer_id,
    status,
    created_at,
    payment_count
)
WITH params AS (
    SELECT TIMESTAMPTZ '2026-09-01 00:00:00+00' AS base_date
),
customer_orders AS (
    SELECT
        c.id AS customer_id,
        generate_series(
            1,
            1 + floor(random() * 10)::INTEGER
        ) AS order_number
    FROM customers AS c
)
SELECT
    row_number() OVER (
        ORDER BY customer_id, order_number
    )::INTEGER,

    customer_id,

    (
        ARRAY[
            'new',
            'paid',
            'processing',
            'shipped',
            'delivered',
            'cancelled'
        ]
    )[1 + floor(random() * 6)::INTEGER],

    base_date
        - floor(random() * 180)::INTEGER * INTERVAL '1 day'
        - floor(random() * 24)::INTEGER * INTERVAL '1 hour',

    1 + floor(random() * 3)::INTEGER

FROM customer_orders
CROSS JOIN params;

-- -------------------------------------------------------
-- seed_orders_items
-- -------------------------------------------------------

INSERT INTO seed_order_items(
	seed_order_id,
	product_id,
	quantity,
	unit_price
)
SELECT 
	o.seed_order_id,
	p.id,
	1 + floor(random() * 5)::integer,
	p.price
FROM seed_orders AS o
CROSS JOIN LATERAL (
	SELECT
		id,
		price
	FROM products
	ORDER BY random()
	LIMIT 1 + floor(random() * 10)::integer
) AS p;

-- -------------------------------------------------------
-- temporary seed_key
-- -------------------------------------------------------

ALTER TABLE orders
ADD COLUMN seed_order_id integer;

-- -------------------------------------------------------
-- insert orders
-- -------------------------------------------------------

WITH inserted_orders AS (
	INSERT INTO orders (
		customer_id,
		status,
		total_amount,
		created_at,
		updated_at,
		seed_order_id
	)
	SELECT
		o.customer_id,
		o.status,
		SUM(i.quantity * i.unit_price)::NUMERIC(12, 2),
		o.created_at,
		o.created_at,
		o.seed_order_id
	FROM seed_orders o
	JOIN seed_order_items i
		ON o.seed_order_id = i.seed_order_id
	GROUP BY
		o.customer_id,
		o.status,
		o.created_at,
		o.seed_order_id

	RETURNING
		id,
		seed_order_id
)

INSERT INTO seed_order_map (
	seed_order_id,
	order_id
)
SELECT 
	seed_order_id,
	id
FROM inserted_orders;

-- -------------------------------------------------------
-- insert order items
-- -------------------------------------------------------

INSERT INTO order_items (
	order_id,
	product_id,
	quantity,
	unit_price
)
SELECT
	m.order_id,
	i.product_id,
	i.quantity,
	i.unit_price
FROM seed_order_items i
JOIN seed_order_map m
	ON i.seed_order_id = m.seed_order_id;

-- =======================================================
-- Payments
-- =======================================================

INSERT INTO payments (
    order_id,
    payment_method,
    status,
    amount,
    created_at,
    updated_at
)
SELECT
    m.order_id,

    methods.payment_methods[p.payment_number],

    CASE
        WHEN p.payment_number = s.payment_count
            THEN 'successful'

        WHEN s.payment_count = 3
             AND p.payment_number = 2
            THEN (
                ARRAY['failed', 'pending']
            )[1 + floor(random() * 2)::INTEGER]

        ELSE 'failed'
    END,

    o.total_amount,

    o.created_at
        + (p.payment_number * 30) * INTERVAL '1 minute',

    o.created_at
        + (p.payment_number * 30) * INTERVAL '1 minute'

FROM seed_order_map AS m

JOIN seed_orders AS s
    ON s.seed_order_id = m.seed_order_id

JOIN orders AS o
    ON o.id = m.order_id

CROSS JOIN LATERAL (
    SELECT
        ARRAY_AGG(payment_method ORDER BY random()) AS payment_methods
    FROM unnest(
        ARRAY[
            'card',
            'cash',
            'bank_transfer'
        ]
    ) AS payment_method
) AS methods

CROSS JOIN LATERAL generate_series(
    1,
    s.payment_count
) AS p(payment_number);

-- ============================================================
-- Remove temporary seed key
-- ============================================================

ALTER TABLE orders
DROP COLUMN seed_order_id;

-- ============================================================
-- Cleanup
-- ============================================================

DROP TABLE seed_order_items;
DROP TABLE seed_order_map;
DROP TABLE seed_orders;