"""Show the compatibility rule at work: v2 is accepted, the breaking v3 is refused."""
from .codec import load_schema
from .registry import CompatibilityError, Registry

ARTIFACT = "OrderPlaced"

registry = Registry()
registry.register(ARTIFACT, load_schema("order_v1.avsc"))
registry.require_backward_compatibility(ARTIFACT)

print("v2 adds 'currency' with a default:", registry.register(ARTIFACT, load_schema("order_v2.avsc")))
try:
    registry.register(ARTIFACT, load_schema("order_v3_breaking.avsc"))
except CompatibilityError as e:
    print("v3 adds 'channel' with no default: refused by the registry")
    print(" ", e)
