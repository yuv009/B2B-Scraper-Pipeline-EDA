import pandas as pd
import json
import os
from urllib.parse import urljoin

def get_nested_value(data_list, label_name):
    """
    Safely searches for a value in a list of dictionaries like 'Trade_Information' 
    or 'Product_Specifications'.
    """
    if not isinstance(data_list, list):
        return None
    for item in data_list:
        if isinstance(item, dict) and label_name.lower() in item.get('label_name', '').strip().lower():
            return item.get('value')
    return None

def parse_product_data(raw_product_data, sub_category_name, main_category_name):
    """
    Parses a single raw product dictionary and extracts all the key metrics
    into a clean, flat dictionary.
    """
    custom_info = raw_product_data.get('custom_field_data_meta_info')
    
    trade_info = []
    product_specs = []
    price_quantity_info = []

    if isinstance(custom_info, dict):
        trade_info = custom_info.get('Trade_Information', [])
        product_specs = custom_info.get('Product_Specifications', [])
        price_quantity_info = custom_info.get('Price_And_Quantity', [])

    parsed_data = {
        'product_id': raw_product_data.get('product_id'),
        'profile_id': raw_product_data.get('profile_id'),
        'userid': raw_product_data.get('userid'),
        'product_name': raw_product_data.get('product_name'),
        'co_name': raw_product_data.get('co_name'),
        'prod_url': f"https://www.tradeindia.com{raw_product_data.get('prod_url', '')}",
        'profile_url': f"https://www.tradeindia.com{raw_product_data.get('profile_url', '')}",
        'main_category': main_category_name,
        'sub_category': sub_category_name,
        'member_since': raw_product_data.get('member_since'),
        'year_established': raw_product_data.get('estd'),
        'has_trust_stamp': raw_product_data.get('has_trust_stamp'),
        'trust_stamp_url': f"https://www.tradeindia.com{raw_product_data.get('trust_stamp_url', '')}" if raw_product_data.get('trust_stamp_url') else None,
        'certifications': raw_product_data.get('std_cert'),
        'last_verified': raw_product_data.get('last_verified'),
        'city': raw_product_data.get('city'),
        'state': raw_product_data.get('state'),
        'country_name': raw_product_data.get('country_name'),
        'is_manufacturer': raw_product_data.get('ifmanu'),
        'is_distributor': raw_product_data.get('ifdistributor'),
        'is_supplier': raw_product_data.get('ifsupplier'),
        'is_exporter': raw_product_data.get('ifexporter'),
        'is_trader': raw_product_data.get('iftrader'),
        'is_service_provider': raw_product_data.get('ifservice'),
        'supply_ability': get_nested_value(trade_info, 'supply ability'),
        'delivery_time': get_nested_value(trade_info, 'delivery time'),
        'price_string': raw_product_data.get('price'),
        'price_numeric': raw_product_data.get('price_es'),
        'price_range': raw_product_data.get('price_range'),
        'min_price_range': raw_product_data.get('min_price_range'),
        'max_price_range': raw_product_data.get('max_price_range'),
        'product_description': raw_product_data.get('product_description'),
        'keywords': raw_product_data.get('keywords'),
        'price_from_sub_json': get_nested_value(price_quantity_info, 'price'),
        'minimum_order_quantity': get_nested_value(price_quantity_info, 'minimum order quantity'),
        'payment_terms': get_nested_value(trade_info, 'payment terms'),
        'buyer_feedback_score': raw_product_data.get('ua_buyer_feedback'),
        'in_stock': raw_product_data.get('in_stock'),
        'made_in_india': raw_product_data.get('made_in_india')
    }
    return parsed_data