-- ============================================
-- Схема базы данных маркетплейса
-- ============================================

-- Включаем расширение UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE TABLE order_statuses (
    status VARCHAR(20) PRIMARY KEY,
    description TEXT
);

INSERT INTO order_statuses (status, description) VALUES
    ('created', 'Заказ создан'),
    ('paid', 'Заказ оплачен'),
    ('cancelled', 'Заказ отменён'),
    ('shipped', 'Заказ отправлен'),
    ('completed', 'Заказ завершён');


CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_email_not_empty CHECK (email <> ''),
    CONSTRAINT chk_email_valid CHECK (email ~* '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+[.][a-zA-Z0-9.-]+$')
);


CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'created' REFERENCES order_statuses(status),
    total_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_total_amount_non_negative CHECK (total_amount >= 0)
);


CREATE TABLE order_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_name VARCHAR(255) NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    quantity INTEGER NOT NULL,
    CONSTRAINT chk_product_name_not_empty CHECK (product_name <> ''),
    CONSTRAINT chk_price_non_negative CHECK (price >= 0),
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0)
);


CREATE TABLE order_status_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL REFERENCES order_statuses(status),
    changed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION check_order_not_already_paid()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status = 'paid' THEN
        IF EXISTS (
            SELECT 1 FROM order_status_history
            WHERE order_id = NEW.id AND status = 'paid'
        ) THEN
            RAISE EXCEPTION 'Order % is already paid', NEW.id;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_check_order_not_already_paid
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION check_order_not_already_paid();

CREATE OR REPLACE FUNCTION recalculate_order_total()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE orders
    SET total_amount = COALESCE(
        (SELECT SUM(price * quantity) FROM order_items WHERE order_id = NEW.order_id),
        0
    )
    WHERE id = NEW.order_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_recalculate_order_total
    AFTER INSERT OR UPDATE OR DELETE ON order_items
    FOR EACH ROW
    EXECUTE FUNCTION recalculate_order_total();

--Отключаем только для второй лабы

--CREATE OR REPLACE FUNCTION record_status_change()
--RETURNS TRIGGER AS $$
--BEGIN
--    IF OLD.status IS DISTINCT FROM NEW.status THEN
--        INSERT INTO order_status_history (id, order_id, status, changed_at)
--        VALUES (uuid_generate_v4(), NEW.id, NEW.status, NOW());
--    END IF;
--    RETURN NEW;
--END;
--$$ LANGUAGE plpgsql;

--CREATE TRIGGER trigger_record_status_change
--    AFTER UPDATE ON orders
--    FOR EACH ROW
--    EXECUTE FUNCTION record_status_change();

CREATE OR REPLACE FUNCTION record_initial_status()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO order_status_history (id, order_id, status, changed_at)
    VALUES (uuid_generate_v4(), NEW.id, NEW.status, NOW());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_record_initial_status
    AFTER INSERT ON orders
    FOR EACH ROW
    EXECUTE FUNCTION record_initial_status();
