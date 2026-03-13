# Статус лабораторной работы №2

## ✅ Что готово

### Инфраструктура
- ✅ Docker окружение настроено (из lab_01)
- ✅ Backend (FastAPI) - запускается (из lab_01)
- ✅ PostgreSQL - подключена (из lab_01)
- ✅ Базовая схема БД (001_init.sql из lab_01)

### Документация и шаблоны
- ✅ README.md - полное описание задания с фокусом на кодовую реализацию
- ✅ REPORT.md - шаблон отчёта с TODO
- ✅ PaymentService - файл с TODO и подробными комментариями
- ✅ Тесты - шаблоны тестов с инструкциями

## ⚠️ Что нужно реализовать

### 1. PaymentService (`backend/app/application/payment_service.py`)

**pay_order_unsafe()**
- ✅ Реализовать чтение статуса заказа БЕЗ FOR UPDATE
- ✅ Реализовать проверку статуса
- ✅ Реализовать UPDATE orders SET status = 'paid'
- ✅ Реализовать INSERT в order_status_history
- ✅ НЕ использовать FOR UPDATE!
- ✅ НЕ менять уровень изоляции!

**pay_order_safe()**
- ✅ Установить REPEATABLE READ уровень изоляции
- ✅ Реализовать SELECT ... FOR UPDATE для блокировки строки
- ✅ Реализовать проверку статуса
- ✅ Реализовать UPDATE orders SET status = 'paid'
- ✅ Реализовать INSERT в order_status_history
- ✅ Обязательно использовать FOR UPDATE!

**get_payment_history()**
- ✅ Реализовать запрос истории оплат
- ✅ Фильтровать по order_id и status = 'paid'

### 2. Тесты

**test_concurrent_payment_unsafe.py**
- ✅ Создать фикстуру db_session
- ✅ Создать фикстуру test_order
- ✅ Реализовать тест, запускающий два параллельных pay_order_unsafe()
- ✅ Проверить, что в истории ДВЕ записи 'paid' (race condition)
- ✅ Добавить вывод: "⚠️ RACE CONDITION DETECTED!"
- ✅ Тест должен ПРОХОДИТЬ

**test_concurrent_payment_safe.py**
- ✅ Создать фикстуры (аналогично unsafe)
- ✅ Реализовать тест, запускающий два параллельных pay_order_safe()
- ✅ Проверить, что в истории ОДНА запись 'paid'
- ✅ Проверить, что одна попытка успешна, вторая - ошибка
- ✅ Добавить вывод: "✅ RACE CONDITION PREVENTED!"
- ✅ Тест должен ПРОХОДИТЬ

### 3. Отчёт REPORT.md

**Раздел 1: Описание проблемы**
- ✅ Объяснить, что такое race condition
- ✅ Почему READ COMMITTED не защищает
- ✅ Привести примеры из реальной жизни

**Раздел 2: Уровни изоляции**
- ✅ Описать READ UNCOMMITTED
- ✅ Описать READ COMMITTED
- ✅ Описать REPEATABLE READ
- ✅ Описать SERIALIZABLE
- ✅ Сравнительная таблица

**Раздел 3: Решение проблемы**
- ✅ Почему REPEATABLE READ решает проблему
- ✅ Зачем нужен FOR UPDATE
- ✅ Что без FOR UPDATE на REPEATABLE READ
- ✅ Разница FOR UPDATE и FOR SHARE

**Раздел 4: Рекомендации для продакшена**
- ✅ Обосновать выбор ISOLATION LEVEL
- ✅ Анализ производительности
- ✅ Анализ безопасности
- ✅ Альтернативные подходы
- ✅ Итоговая рекомендация

## 🎯 Главное требование

**Продемонстрировать race condition в коде и решить его на уровне СУБД!**

Два метода должны:
1. **pay_order_unsafe()** - ломаться при конкурентных запросах
2. **pay_order_safe()** - работать корректно благодаря REPEATABLE READ + FOR UPDATE

## 🚀 Как начать

```bash
# 1. Запустить проект
cd lab_02
docker-compose up --build

# 2. Реализовать PaymentService
# Откройте backend/app/application/payment_service.py
# Следуйте TODO комментариям

# 3. Реализовать тесты
# Откройте backend/app/tests/test_concurrent_payment_*.py
# Следуйте TODO комментариям

# 4. Запустить тесты
cd backend
export PYTHONPATH=$(pwd)
pytest app/tests/test_concurrent_payment_unsafe.py -v -s
pytest app/tests/test_concurrent_payment_safe.py -v -s

# 5. Заполнить отчёт REPORT.md
```

## 📝 Порядок выполнения

Рекомендуемый порядок:

1. Изучите payment_service.py - там подробные TODO
2. Реализуйте pay_order_unsafe() (простая версия БЕЗ блокировок)
3. Реализуйте get_payment_history() (для проверки)
4. Реализуйте тест test_concurrent_payment_unsafe.py
5. Запустите тест - убедитесь, что заказ оплачивается дважды ⚠️
6. Реализуйте pay_order_safe() (с REPEATABLE READ + FOR UPDATE)
7. Реализуйте тест test_concurrent_payment_safe.py
8. Запустите тест - убедитесь, что заказ оплачивается один раз ✅
9. Заполните отчёт REPORT.md

## ✅ Проверка

После выполнения тестов проверьте:

```bash
# Тест должен ПРОЙТИ и показать двойную оплату
pytest app/tests/test_concurrent_payment_unsafe.py -v -s
# Ожидаемый вывод:
# ⚠️ RACE CONDITION DETECTED!
# Order XXX was paid TWICE

# Тест должен ПРОЙТИ и показать однократную оплату
pytest app/tests/test_concurrent_payment_safe.py -v -s
# Ожидаемый вывод:
# ✅ RACE CONDITION PREVENTED!
# Order XXX was paid only ONCE
```

## 📚 Документация

- `README.md` - полное описание задания
- `REPORT.md` - шаблон отчёта с TODO
- `payment_service.py` - сервис с подробными TODO
- `test_concurrent_payment_*.py` - тесты с инструкциями
