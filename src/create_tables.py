from src.database import Base, engine
from src.models import OrderItem, CleanData, FaultyData      # noqa: F401
from src.models1 import Shipment      # noqa: F401  – registers table

Base.metadata.create_all(bind=engine)
print("✅ Tables created (items, clean_data, faulty_data, items1).")