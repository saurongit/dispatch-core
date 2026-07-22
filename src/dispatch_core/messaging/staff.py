from __future__ import annotations

from typing import Any

from dispatch_core.application.identity import ActorIdentity
from dispatch_core.infrastructure.workflow_store import (
    PostgresStaffRoleSelectionStore,
)
from dispatch_core.messaging.replies import Reply, ReplyButton

STAFF_SELECT_ROLE = "staff_select_role"
STAFF_ACTIONS = frozenset({STAFF_SELECT_ROLE})

_ROLE_ORDER = ("admin", "operator", "master")
_ROLE_LABELS = {
    "admin": "🛡 Администратор",
    "operator": "🧭 Оператор",
    "master": "🧰 Мастер",
}


class StaffRoleCoordinator:
    """Explicit effective-role selection for the shared MAX staff bot."""

    def __init__(self, selections: PostgresStaffRoleSelectionStore) -> None:
        self._selections = selections

    async def start(self, identity: ActorIdentity) -> Reply:
        await self.clear(identity)
        return self.menu(identity)

    def menu(self, identity: ActorIdentity, *, note: str | None = None) -> Reply:
        roles = self.available_roles(identity)
        if not roles:
            return Reply(
                "У этого аккаунта нет служебных ролей. Обратитесь к администратору."
            )
        lines: list[str] = []
        if note:
            lines.extend((note, ""))
        lines.extend(
            (
                f"Здравствуйте, {identity.display_name}!",
                "Выберите, в какой роли открыть диспетчерскую:",
            )
        )
        buttons = tuple(
            ReplyButton(
                _ROLE_LABELS[role],
                STAFF_SELECT_ROLE,
                {"role": role},
                allowed_role=None,
                row=index,
            )
            for index, role in enumerate(roles)
        )
        return Reply("\n\n".join(lines), buttons=buttons)

    async def selected(self, identity: ActorIdentity) -> str | None:
        return await self._selections.get(
            organization_id=identity.organization_id,
            provider=identity.provider,
            external_user_id=identity.external_user_id,
        )

    async def select(
        self,
        identity: ActorIdentity,
        payload: dict[str, Any],
    ) -> tuple[str | None, Reply]:
        role = str(payload.get("role") or "")
        if role not in self.available_roles(identity):
            await self.clear(identity)
            return None, self.menu(
                identity,
                note="Эта роль больше недоступна. Выберите актуальную.",
            )
        await self._selections.put(
            organization_id=identity.organization_id,
            provider=identity.provider,
            external_user_id=identity.external_user_id,
            role=role,
        )
        return role, Reply(
            f"Режим «{_ROLE_LABELS[role].split(maxsplit=1)[1]}» включён. "
            "Чтобы сменить роль, снова нажмите /start."
        )

    async def clear(self, identity: ActorIdentity) -> None:
        await self._selections.clear(
            organization_id=identity.organization_id,
            provider=identity.provider,
            external_user_id=identity.external_user_id,
        )

    @staticmethod
    def available_roles(identity: ActorIdentity) -> tuple[str, ...]:
        roles = identity.roles or frozenset({identity.role})
        return tuple(role for role in _ROLE_ORDER if role in roles)
