from sqlalchemy import Column, Integer, String, Float
from .database import Base 

class Shipment(Base):
    __tablename__ = "items1" 

    id = Column(Integer, primary_key=True)
    warehouse_block = Column(String)
    mode_of_shipment = Column(String)
    customer_care_calls = Column(Integer)
    customer_rating = Column(Float)
    cost_of_the_product = Column(Float)
    prior_purchases = Column(Integer)
    product_importance = Column(String)
    gender = Column(String)
    discount_offered = Column(Integer)
    weight_in_gms = Column(Integer)
    reached_on_time_y_n = Column(Integer) 
    