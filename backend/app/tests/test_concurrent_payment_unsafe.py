"""
Тест для демонстрации ПРОБЛЕМЫ race condition.

Этот тест должен ПРОХОДИТЬ, подтверждая, что при использовании
pay_order_unsafe() возникает двойная оплата.
"""

import asyncio
import pytest
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import text

from app.application.payment_service import PaymentService


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
    
    Реализация:
    1. Создать тестового пользователя
    2. Создать тестовый заказ со статусом 'created'
    3. Записать начальный статус в историю
    4. Вернуть order_id
    5. После теста - очистить данные
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


@pytest.mark.asyncio
async def test_concurrent_payment_unsafe_demonstrates_race_condition(db_session, test_order):
    """
    Тест демонстрирует проблему race condition при использовании pay_order_unsafe().
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ: Тест ПРОХОДИТ, подтверждая, что заказ был оплачен дважды.
    Это показывает, что метод pay_order_unsafe() НЕ защищен от конкурентных запросов.
    
    Реализация:
    
    1. Создать два экземпляра PaymentService с РАЗНЫМИ сессиями
       (это имитирует два независимых HTTP-запроса)
       
    2. Запустить два параллельных вызова pay_order_unsafe():
       
       async def payment_attempt_1():
           service1 = PaymentService(session1)
           return await service1.pay_order_unsafe(order_id)
           
       async def payment_attempt_2():
           service2 = PaymentService(session2)
           return await service2.pay_order_unsafe(order_id)
           
       results = await asyncio.gather(
           payment_attempt_1(),
           payment_attempt_2(),
           return_exceptions=True
       )
       
    3. Проверить историю оплат:
       
       service = PaymentService(session)
       history = await service.get_payment_history(order_id)
       
       # ОЖИДАЕМ ДВЕ ЗАПИСИ 'paid' - это и есть проблема!
       assert len(history) == 2, "Ожидалось 2 записи об оплате (RACE CONDITION!)"
       
    4. Вывести информацию о проблеме:
       
       print(f"⚠️ RACE CONDITION DETECTED!")
       print(f"Order {order_id} was paid TWICE:")
       for record in history:
           print(f"  - {record['changed_at']}: status = {record['status']}")
    """

    order_id = test_order

    engine1 = create_async_engine(DATABASE_URL)
    engine2 = create_async_engine(DATABASE_URL)

    async def payment_attempt_1():
        """Первая попытка оплаты через независимое соединение."""
        async with AsyncSession(engine1) as session1:
            service = PaymentService(session1)
            return await service.pay_order_unsafe(order_id)

    async def payment_attempt_2():
        """Вторая попытка оплаты через независимое соединение."""
        async with AsyncSession(engine2) as session2:
            service = PaymentService(session2)
            return await service.pay_order_unsafe(order_id)

    results = await asyncio.gather(
        payment_attempt_1(),
        payment_attempt_2(),
        return_exceptions=True
    )

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"  Попытка {i+1} завершилась ошибкой: {result}")
        else:
            print(f"  Попытка {i+1} успешна: {result}")

    engine3 = create_async_engine(DATABASE_URL)
    async with AsyncSession(engine3) as session3:
        service = PaymentService(session3)
        history = await service.get_payment_history(order_id)

    await engine1.dispose()
    await engine2.dispose()
    await engine3.dispose()

    print(f"\n{'='*60}")
    print(f"⚠️  RACE CONDITION DETECTED!")
    print(f"Order {order_id} was paid TWICE:")
    for record in history:
        print(f"  - {record['changed_at']}: status = {record['status']}")
    print(f"{'='*60}")

    assert len(history) == 2, (
        f"Ожидалось 2 записи об оплате (RACE CONDITION!), "
        f"но получено {len(history)}"
    )


@pytest.mark.asyncio
async def test_concurrent_payment_unsafe_both_succeed(db_session, test_order):
    """
    Дополнительный тест: проверить, что ОБЕ транзакции успешно завершились.
    
    Реализация:
    1. Обе попытки оплаты вернули успешный результат
    2. Ни одна не выбросила исключение
    3. Обе записали в историю
    
    Это подтверждает, что проблема не в ошибках, а в race condition.
    """

    order_id = test_order

    engine1 = create_async_engine(DATABASE_URL)
    engine2 = create_async_engine(DATABASE_URL)

    async def payment_attempt_1():
        async with AsyncSession(engine1) as session1:
            service = PaymentService(session1)
            return await service.pay_order_unsafe(order_id)

    async def payment_attempt_2():
        async with AsyncSession(engine2) as session2:
            service = PaymentService(session2)
            return await service.pay_order_unsafe(order_id)

    results = await asyncio.gather(
        payment_attempt_1(),
        payment_attempt_2(),
        return_exceptions=True
    )

    await engine1.dispose()
    await engine2.dispose()

    successes = [r for r in results if not isinstance(r, Exception)]
    errors = [r for r in results if isinstance(r, Exception)]

    print(f"\n  Успешных попыток: {len(successes)}")
    print(f"  Ошибок: {len(errors)}")

    assert len(successes) == 2, (
        f"Ожидалось 2 успешные попытки оплаты, но {len(errors)} завершились ошибкой: "
        f"{[str(e) for e in errors]}"
    )
    assert len(errors) == 0, (
        f"Ожидалось 0 ошибок, но получено {len(errors)}: "
        f"{[str(e) for e in errors]}"
    )


if __name__ == "__main__":
    """
    Запуск теста:
    
    cd backend
    export PYTHONPATH=$(pwd)
    pytest app/tests/test_concurrent_payment_unsafe.py -v -s
    
    ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
    ✅ test_concurrent_payment_unsafe_demonstrates_race_condition PASSED
    
    Вывод должен показывать:
    ⚠️ RACE CONDITION DETECTED!
    Order XXX was paid TWICE:
      - 2024-XX-XX: status = paid
      - 2024-XX-XX: status = paid
    """
    pytest.main([__file__, "-v", "-s"])
