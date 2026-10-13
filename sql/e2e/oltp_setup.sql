INSERT INTO customers (
    email,
    first_name,
    last_name
)
VALUES (
    :'customer_email',
    'Alice',
    'e2e'
)
RETURNING id as customer_id
\gset

INSERT INTO products (
    name,
    category,
    price
)
VALUES (
    'e2e_product',
    'e2e_category',
    100
)
RETURNING id as product_id
\gset

INSERT INTO orders (
    customer_id,
    status,
    total_amount
)
VALUES (
    :customer_id,
    'new',
    200
)
RETURNING id as order_id
\gset

INSERT INTO order_items (
    order_id,
    product_id,
    quantity,
    unit_price
)
VALUES (
    :order_id,
    :product_id,
    2,
    100
)
RETURNING id as order_item_id
\gset


SELECT :customer_id, :product_id, :order_id, :order_item_id;
