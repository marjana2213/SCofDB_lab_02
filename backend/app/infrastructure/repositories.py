"""Реализация репозиториев с использованием SQLAlchemy."""

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.user import User
from app.domain.order import Order, OrderItem, OrderStatus, OrderStatusChange


class UserRepository:
    """Репозиторий для User."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def save(self, user: User) -> None:
        await self.session.execute(
            text("""
                INSERT INTO users (id, email, name, created_at)
                VALUES (:id, :email, :name, :created_at)
                ON CONFLICT (id) DO UPDATE
                SET email = :email, name = :name
            """),
            {
                "id": str(user.id),
                "email": user.email,
                "name": user.name,
                "created_at": user.created_at,
            },
        )
        await self.session.commit()

    async def find_by_id(self, user_id: uuid.UUID) -> Optional[User]:
        result = await self.session.execute(
            text("SELECT id, email, name, created_at FROM users WHERE id = :id"),
            {"id": str(user_id)},
        )
        row = result.fetchone()
        if row is None:
            return None
        return self._row_to_user(row)

    async def find_by_email(self, email: str) -> Optional[User]:
        result = await self.session.execute(
            text("SELECT id, email, name, created_at FROM users WHERE email = :email"),
            {"email": email},
        )
        row = result.fetchone()
        if row is None:
            return None
        return self._row_to_user(row)

    async def find_all(self) -> List[User]:
        result = await self.session.execute(
            text("SELECT id, email, name, created_at FROM users ORDER BY created_at DESC")
        )
        return [self._row_to_user(row) for row in result.fetchall()]

    def _row_to_user(self, row) -> User:
        u = object.__new__(User)
        u.id = row[0] if isinstance(row[0], uuid.UUID) else uuid.UUID(str(row[0]))
        u.email = row[1]
        u.name = row[2]
        u.created_at = row[3]
        return u


class OrderRepository:
    """Репозиторий для Order."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def save(self, order: Order) -> None:
        await self.session.execute(
            text("""
                INSERT INTO orders (id, user_id, status, total_amount, created_at)
                VALUES (:id, :user_id, :status, :total_amount, :created_at)
                ON CONFLICT (id) DO UPDATE
                SET status = :status, total_amount = :total_amount
            """),
            {
                "id": str(order.id),
                "user_id": str(order.user_id),
                "status": order.status.value,
                "total_amount": float(order.total_amount),
                "created_at": order.created_at,
            },
        )

        for item in order.items:
            await self.session.execute(
                text("""
                    INSERT INTO order_items (id, order_id, product_name, price, quantity)
                    VALUES (:id, :order_id, :product_name, :price, :quantity)
                    ON CONFLICT (id) DO NOTHING
                """),
                {
                    "id": str(item.id),
                    "order_id": str(order.id),
                    "product_name": item.product_name,
                    "price": float(item.price),
                    "quantity": item.quantity,
                },
            )

        await self.session.commit()

    async def find_by_id(self, order_id: uuid.UUID) -> Optional[Order]:
        result = await self.session.execute(
            text("SELECT id, user_id, status, total_amount, created_at FROM orders WHERE id = :id"),
            {"id": str(order_id)},
        )
        row = result.fetchone()
        if row is None:
            return None

        order = object.__new__(Order)
        order.id = row[0] if isinstance(row[0], uuid.UUID) else uuid.UUID(str(row[0]))
        order.user_id = row[1] if isinstance(row[1], uuid.UUID) else uuid.UUID(str(row[1]))
        order.status = OrderStatus(row[2])
        order.total_amount = Decimal(str(row[3]))
        order.created_at = row[4]

        items_result = await self.session.execute(
            text("SELECT id, order_id, product_name, price, quantity FROM order_items WHERE order_id = :order_id"),
            {"order_id": str(order_id)},
        )
        order.items = []
        for irow in items_result.fetchall():
            item = object.__new__(OrderItem)
            item.id = irow[0] if isinstance(irow[0], uuid.UUID) else uuid.UUID(str(irow[0]))
            item.order_id = irow[1] if isinstance(irow[1], uuid.UUID) else uuid.UUID(str(irow[1]))
            item.product_name = irow[2]
            item.price = Decimal(str(irow[3]))
            item.quantity = irow[4]
            order.items.append(item)

        history_result = await self.session.execute(
            text("SELECT id, order_id, status, changed_at FROM order_status_history WHERE order_id = :order_id ORDER BY changed_at"),
            {"order_id": str(order_id)},
        )
        order.status_history = []
        for hrow in history_result.fetchall():
            change = object.__new__(OrderStatusChange)
            change.id = hrow[0] if isinstance(hrow[0], uuid.UUID) else uuid.UUID(str(hrow[0]))
            change.order_id = hrow[1] if isinstance(hrow[1], uuid.UUID) else uuid.UUID(str(hrow[1]))
            change.status = OrderStatus(hrow[2])
            change.changed_at = hrow[3]
            order.status_history.append(change)

        return order

    async def find_by_user(self, user_id: uuid.UUID) -> List[Order]:
        result = await self.session.execute(
            text("SELECT id FROM orders WHERE user_id = :user_id ORDER BY created_at DESC"),
            {"user_id": str(user_id)},
        )
        orders = []
        for row in result.fetchall():
            oid = row[0] if isinstance(row[0], uuid.UUID) else uuid.UUID(str(row[0]))
            order = await self.find_by_id(oid)
            if order:
                orders.append(order)
        return orders

    async def find_all(self) -> List[Order]:
        result = await self.session.execute(
            text("SELECT id FROM orders ORDER BY created_at DESC")
        )
        orders = []
        for row in result.fetchall():
            oid = row[0] if isinstance(row[0], uuid.UUID) else uuid.UUID(str(row[0]))
            order = await self.find_by_id(oid)
            if order:
                orders.append(order)
        return orders
