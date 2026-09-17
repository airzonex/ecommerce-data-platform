CREATE TABLE customers(
		id			bigint generated always as identity primary key,
		email		text not null unique,
		first_name	text not null,
		last_name	text not null,
		created_at	timestamptz not null default now(),
		updated_at	timestamptz not null default now()
);

CREATE TABLE products(
		id			bigint generated always as identity primary key,
		name		text not null,
		category	text not null,
		price		numeric(12, 2) not null check (price >= 0),
		created_at	timestamptz not null default now(),
		updated_at	timestamptz not null default now()
);

CREATE TABLE orders(
		id				bigint generated always as identity primary key,
		customer_id		bigint not null references customers (id),
		status			text not null check (
				status in (
						'new',
						'paid',
						'processing',
						'shipped',
						'delivered',
						'cancelled'
						)
				),
		total_amount	numeric(12, 2) not null check (total_amount >= 0),
		created_at		timestamptz not null default now(),
		updated_at		timestamptz not null default now()
);

CREATE TABLE order_items(
		id			bigint generated always as identity primary key,
		order_id	bigint not null references orders(id),
		product_id	bigint not null references products(id),
		quantity	integer not null check (quantity > 0),
		unit_price	numeric(12, 2) not null check (unit_price >= 0),
		constraint uq_order_items_order_product unique (order_id, product_id)
);

CREATE TABLE payments(
		id				bigint generated always as identity primary key,
		order_id		bigint not null references orders(id),
		payment_method	text not null check (payment_method in ('card', 'cash', 'bank_transfer')),
		status			text not null check (status in ('failed', 'successful', 'pending')),
		amount			numeric(12, 2) not null check (amount >= 0),
		created_at		timestamptz not null default now(),
		updated_at		timestamptz not null default now()
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
CREATE INDEX idx_payments_order_id ON payments(order_id);
