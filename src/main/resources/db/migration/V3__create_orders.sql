-- Reference schema for the order domain. Strategy A's schema tool inspects this table, and the
-- Evaluator Gate enforces the same rules in Java. Keep the two in sync.
CREATE TABLE orders (
    order_id    VARCHAR(64)   PRIMARY KEY,
    customer_id VARCHAR(64)   NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,
    currency    VARCHAR(3)    NOT NULL,
    status      VARCHAR(16)   NOT NULL,
    created_at  TIMESTAMPTZ   NOT NULL,
    CONSTRAINT chk_orders_amount   CHECK (amount > 0),
    CONSTRAINT chk_orders_currency CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT chk_orders_status   CHECK (status IN ('NEW', 'PAID', 'SHIPPED', 'CANCELLED'))
);
