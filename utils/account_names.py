import re
from typing import Annotated

from pydantic import BeforeValidator


SAFE_ACCOUNT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
INTERNAL_ACCOUNT_PREFIX = "__"


def validate_account_name(account_name: str) -> str:
    """Reject unsafe or process-internal account namespaces with a typed 400."""
    from models.connectors import CapabilityErrorDetail
    from services.connector_capabilities import ConnectorCapabilityError

    if (
        not isinstance(account_name, str)
        or not account_name
        or SAFE_ACCOUNT_NAME_PATTERN.fullmatch(account_name) is None
    ):
        raise ConnectorCapabilityError(
            CapabilityErrorDetail(
                code="invalid_account_resource_name",
                message=(
                    "Invalid account name. Only letters, numbers, underscores "
                    "and hyphens are allowed."
                ),
                connector=account_name or "account",
                feature="account_mutation",
                reason="invalid_name",
            )
        )
    if account_name.startswith(INTERNAL_ACCOUNT_PREFIX):
        raise ConnectorCapabilityError(
            CapabilityErrorDetail(
                code="invalid_account_resource_name",
                message="Invalid account name. Internal namespaces are reserved.",
                connector=account_name,
                feature="account_mutation",
                reason="reserved_name",
            )
        )
    return account_name


AccountName = Annotated[str, BeforeValidator(validate_account_name)]
