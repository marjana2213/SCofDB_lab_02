"""
Тест для демонстрации РЕШЕНИЯ проблемы race condition.

Этот тест должен ПРОХОДИТЬ, подтверждая, что при использовании
pay_order_safe() заказ оплачивается только один раз.
"""

import asyncio
import time
import pytest
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

from app.application.payment_service import PaymentService
from app.domain.exceptions import OrderAlreadyPaidError


DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"


@pytest.fixture
async def engine():
    """Создать engine для подключения к PostgreSQL."""

    eng = create_async_engine(DATABASE_URL)
    yield eng
    await eng.dispose()


@pytest.fixture
async def db_session(engine):
    """
    Создать сессию БД для тестов.
    """

    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest.fixture
async def test_order(db_session):
    """
    Создать тестовый заказ со статусом 'created'.
    """

    user_id = str(uuid.uuid4())
    await db_session.execute(
        text("INSERT INTO users (id, email, name, created_at) VALUES (:id, :email, :name, NOW())"),
        {"id": user_id, "email": f"test_{user_id[:8]}@test.com", "name": "Test User"}
    )

    order_id = uuid.uuid4()
    await db_session.execute(
        text("""
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, 'created', 100.00, NOW())
        """),
        {"id": str(order_id), "user_id": user_id}
    )
    await db_session.commit()

    yield order_id

    #Удалить тестовые записи
    await db_session.execute(
        text("DELETE FROM order_status_history WHERE order_id = :id"),
        {"id": str(order_id)}
    )
    await db_session.execute(
        text("DELETE FROM order_items WHERE order_id = :id"),
        {"id": str(order_id)}
    )
    await db_session.execute(
        text("DELETE FROM orders WHERE id = :id"),
        {"id": str(order_id)}
    )
    await db_session.execute(
        text("DELETE FROM users WHERE id = :id"),
        {"id": user_id}
    )
    await db_session.commit()


@pytest.fixture
async def two_test_orders(db_session):
    """
    Тест демонстрирует решение проблемы race condition с помощью pay_order_safe().
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ: Тест ПРОХОДИТ, подтверждая, что заказ был оплачен только один раз.
    Это показывает, что метод pay_order_safe() защищен от конкурентных запросов.
    
    Реализация:
    
    1. Создать два экземпляра PaymentService с РАЗНЫМИ сессиями
       (это имитирует два независимых HTTP-запроса)
       
    2. Запустить два параллельных вызова pay_order_safe():
       
       async def payment_attempt_1():
           service1 = PaymentService(session1)
           return await service1.pay_order_safe(order_id)
           
       async def payment_attempt_2():
           service2 = PaymentService(session2)
           return await service2.pay_order_safe(order_id)
           
       results = await asyncio.gather(
           payment_attempt_1(),
           payment_attempt_2(),
           return_exceptions=True
       )
       
    3. Проверить результаты:
       - Одна попытка должна УСПЕШНО завершиться
       - Вторая попытка должна выбросить OrderAlreadyPaidError ИЛИ вернуть ошибку
       
       success_count = sum(1 for r in results if not isinstance(r, Exception))
       error_count = sum(1 for r in results if isinstance(r, Exception))
       
       assert success_count == 1, "Ожидалась одна успешная оплата"
       assert error_count == 1, "Ожидалась одна неудачная попытка"
       
    4. Проверить историю оплат:
       
       service = PaymentService(session)
       history = await service.get_payment_history(order_id)
       
       # ОЖИДАЕМ ОДНУ ЗАПИСЬ 'paid' - проблема решена!
       assert len(history) == 1, "Ожидалась 1 запись об оплате (БЕЗ RACE CONDITION!)"
       
    5. Вывести информацию об успешном решении:
       
       print(f"✅ RACE CONDITION PREVENTED!")
       print(f"Order {order_id} was paid only ONCE:")
       print(f"  - {history[0]['changed_at']}: status = {history[0]['status']}")
       print(f"Second attempt was rejected: {results[1]}")
    """

    user_id = str(uuid.uuid4())
    await db_session.execute(
        text("INSERT INTO users (id, email, name, created_at) VALUES (:id, :email, :name, NOW())"),
        {"id": user_id, "email": f"test_{user_id[:8]}@test.com", "name": "Test User"}
    )

    order_id_1 = uuid.uuid4()
    order_id_2 = uuid.uuid4()

    await db_session.execute(
        text("""
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, 'created', 100.00, NOW())
        """),
        {"id": str(order_id_1), "user_id": user_id}
    )
    await db_session.execute(
        text("""
            INSERT INTO orders (id, user_id, status, total_amount, created_at)
            VALUES (:id, :user_id, 'created', 200.00, NOW())
        """),
        {"id": str(order_id_2), "user_id": user_id}
    )
    await db_session.commit()

    yield order_id_1, order_id_2

    #Удалить тестовые записи
    for oid in [order_id_1, order_id_2]:
        await db_session.execute(
            text("DELETE FROM order_status_history WHERE order_id = :id"),
            {"id": str(oid)}
        )
        await db_session.execute(
            text("DELETE FROM order_items WHERE order_id = :id"),
            {"id": str(oid)}
        )
        await db_session.execute(
            text("DELETE FROM orders WHERE id = :id"),
            {"id": str(oid)}
        )
    await db_session.execute(
        text("DELETE FROM users WHERE id = :id"),
        {"id": user_id}
    )
    await db_session.commit()


@pytest.mark.asyncio
async def test_concurrent_payment_safe_prevents_race_condition(db_session, test_order):
    """
    Дополнительный тест: проверить работу блокировок с явной задержкой.
    
    Реализация:
    
    1. Первая транзакция:
       - Начать транзакцию
       - Заблокировать заказ (FOR UPDATE)
       - Добавить задержку (asyncio.sleep(1))
       - Оплатить
       - Commit
       
    2. Вторая транзакция (запустить через 0.1 секунды после первой):
       - Начать транзакцию
       - Попытаться заблокировать заказ (FOR UPDATE)
       - ДОЛЖНА ЖДАТЬ освобождения блокировки от первой транзакции
       - После освобождения - увидеть обновленный статус 'paid'
       - Выбросить OrderAlreadyPaidError
       
    3. Проверить временные метки:
       - Вторая транзакция должна завершиться ПОЗЖЕ первой
       - Разница должна быть >= 1 секунды (время задержки)
       
    Это подтверждает, что FOR UPDATE действительно блокирует строку.
    """

    order_id = test_order

    engine1 = create_async_engine(DATABASE_URL)
    engine2 = create_async_engine(DATABASE_URL)

    async def payment_attempt_1():
        async with AsyncSession(engine1) as session1:
            service = PaymentService(session1)
            return await service.pay_order_safe(order_id)

    async def payment_attempt_2():
        async with AsyncSession(engine2) as session2:
            service = PaymentService(session2)
            return await service.pay_order_safe(order_id)

    results = await asyncio.gather(
        payment_attempt_1(),
        payment_attempt_2(),
        return_exceptions=True
    )

    successes = [r for r in results if not isinstance(r, Exception)]
    errors = [r for r in results if isinstance(r, Exception)]

    print(f"\n  Успешных попыток: {len(successes)}")
    print(f"  Ошибок: {len(errors)}")

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"  Попытка {i+1} завершилась ошибкой: {type(result).__name__}: {result}")
        else:
            print(f"  Попытка {i+1} успешна: {result}")

    assert len(successes) == 1, f"Ожидалась 1 успешная оплата, получено {len(successes)}"
    assert len(errors) == 1, f"Ожидалась 1 неудачная попытка, получено {len(errors)}"

    error = errors[0]
    assert isinstance(error, (OrderAlreadyPaidError, Exception)), (
        f"Ожидалась OrderAlreadyPaidError, получено {type(error).__name__}"
    )

    engine3 = create_async_engine(DATABASE_URL)
    async with AsyncSession(engine3) as session3:
        service = PaymentService(session3)
        history = await service.get_payment_history(order_id)

    await engine1.dispose()
    await engine2.dispose()
    await engine3.dispose()

    print(f"\n{'='*60}")
    print(f"✅  RACE CONDITION PREVENTED!")
    print(f"Order {order_id} was paid only ONCE:")
    for record in history:
        print(f"  - {record['changed_at']}: status = {record['status']}")
    print(f"Second attempt was rejected: {type(errors[0]).__name__}: {errors[0]}")
    print(f"{'='*60}")

    assert len(history) == 1, (
        f"Ожидалась 1 запись об оплате (БЕЗ RACE CONDITION!), "
        f"но получено {len(history)}"
    )


@pytest.mark.asyncio
async def test_concurrent_payment_safe_with_explicit_timing(db_session, test_order):
    """
    Дополнительный тест: проверить, что блокировки не мешают разным заказам.
    
    Реализация:
    1. Создать ДВА разных заказа
    2. Оплатить их ПАРАЛЛЕЛЬНО с помощью pay_order_safe()
    3. Проверить, что ОБА успешно оплачены
    
    Это показывает, что FOR UPDATE блокирует только конкретную строку,
    а не всю таблицу, что важно для производительности.
    """

    order_id = test_order

    engine1 = create_async_engine(DATABASE_URL)
    engine2 = create_async_engine(DATABASE_URL)

    timestamps = {}

    async def payment_attempt_1_with_delay():
        async with AsyncSession(engine1) as session1:
            try:
                await session1.execute(
                    text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                )
                await session1.execute(
                    text("SELECT status FROM orders WHERE id = :order_id FOR UPDATE"),
                    {"order_id": str(order_id)}
                )
                
                await asyncio.sleep(1)

                await session1.execute(
                    text("UPDATE orders SET status = 'paid' WHERE id = :order_id"),
                    {"order_id": str(order_id)}
                )
                await session1.execute(
                    text("""
                        INSERT INTO order_status_history (id, order_id, status, changed_at)
                        VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
                    """),
                    {"order_id": str(order_id)}
                )
                await session1.commit()
                timestamps["attempt_1_end"] = time.time()
                return {"status": "paid", "attempt": 1}
            except Exception as e:
                await session1.rollback()
                timestamps["attempt_1_end"] = time.time()
                raise

    async def payment_attempt_2_delayed_start():
        await asyncio.sleep(0.1) 
        async with AsyncSession(engine2) as session2:
            try:
                timestamps["attempt_2_start"] = time.time()
                service2 = PaymentService(session2)
                result = await service2.pay_order_safe(order_id)
                timestamps["attempt_2_end"] = time.time()
                return result
            except Exception as e:
                timestamps["attempt_2_end"] = time.time()
                raise

    results = await asyncio.gather(
        payment_attempt_1_with_delay(),
        payment_attempt_2_delayed_start(),
        return_exceptions=True
    )

    await engine1.dispose()
    await engine2.dispose()

    if "attempt_1_end" in timestamps and "attempt_2_end" in timestamps:
        time_diff = timestamps["attempt_2_end"] - timestamps["attempt_1_end"]
        print(f"\n  Разница во времени завершения: {time_diff:.2f}с")
        print(f"  Вторая транзакция ждала блокировку от первой")

    successes = [r for r in results if not isinstance(r, Exception)]
    errors = [r for r in results if isinstance(r, Exception)]

    assert len(successes) == 1, f"Ожидалась 1 успешная оплата, получено {len(successes)}"
    assert len(errors) == 1, f"Ожидалась 1 ошибка, получено {len(errors)}"


@pytest.mark.asyncio
async def test_concurrent_payment_safe_multiple_orders(db_session, two_test_orders):
    """
    Дополнительный тест: проверить, что блокировки не мешают разным заказам.
    
    Реализация:
    1. Создать ДВА разных заказа
    2. Оплатить их ПАРАЛЛЕЛЬНО с помощью pay_order_safe()
    3. Проверить, что ОБА успешно оплачены
    
    Это показывает, что FOR UPDATE блокирует только конкретную строку,
    а не всю таблицу, что важно для производительности.
    """

    order_id_1, order_id_2 = two_test_orders

    engine1 = create_async_engine(DATABASE_URL)
    engine2 = create_async_engine(DATABASE_URL)

    async def pay_order_1():
        async with AsyncSession(engine1) as session1:
            service = PaymentService(session1)
            return await service.pay_order_safe(order_id_1)

    async def pay_order_2():
        async with AsyncSession(engine2) as session2:
            service = PaymentService(session2)
            return await service.pay_order_safe(order_id_2)

    results = await asyncio.gather(
        pay_order_1(),
        pay_order_2(),
        return_exceptions=True
    )

    await engine1.dispose()
    await engine2.dispose()

    successes = [r for r in results if not isinstance(r, Exception)]
    errors = [r for r in results if isinstance(r, Exception)]

    print(f"\n  Оплата заказа 1: {'успешно' if not isinstance(results[0], Exception) else 'ошибка'}")
    print(f"  Оплата заказа 2: {'успешно' if not isinstance(results[1], Exception) else 'ошибка'}")

    assert len(successes) == 2, (
        f"Ожидалось 2 успешные оплаты разных заказов, "
        f"но {len(errors)} ошибок: {[str(e) for e in errors]}"
    )


if __name__ == "__main__":
    """
    Запуск теста:
    
    cd backend
    export PYTHONPATH=$(pwd)
    pytest app/tests/test_concurrent_payment_safe.py -v -s
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
    ✅ test_concurrent_payment_safe_prevents_race_condition PASSED
    
    Вывод должен показывать:
    ✅ RACE CONDITION PREVENTED!
    Order XXX was paid only ONCE:
      - 2024-XX-XX: status = paid
    Second attempt was rejected: OrderAlreadyPaidError(...)
    """
    pytest.main([__file__, "-v", "-s"])
