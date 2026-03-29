from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class UserContext:
    last_intent: Optional[str] = None
    last_item: Optional[str] = None
    cart: Dict[str, int] = field(default_factory=dict)
