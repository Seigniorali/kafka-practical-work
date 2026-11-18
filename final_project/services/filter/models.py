import faust


class Product(faust.Record, serializer="json"):
    product_id: str
    name: str
    description: str
    price: dict
    category: str
    brand: str
    stock: dict
    sku: str
    tags: list
    images: list
    specifications: dict
    created_at: str
    updated_at: str
    index: str
    store_id: str


class BlockEvent(faust.Record, serializer="json"):
    product_id: str
    action: str