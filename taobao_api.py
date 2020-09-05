# /usr/bin/env python
# -*- coding: utf8 -*-
# @File: taobao_api.py
# @Time: 2020/5/29 20:28
# @Author: Wbrikor
# @Desc:


from datetime import datetime
import requests
from utils import make_sign, concat_params
from taobao_table import DBSession, Base, engine, TaoBao_Trade_Order, TaoBao_Trade_Order_Detail
import time


class TaobaoRequest(object):
    """

    """
    Base.metadata.create_all(engine)

    def __init__(self, app_key, session, app_secret):
        self.__APP_KEY = app_key
        self.__SESSION = session
        self.__FORMAT = 'json'
        self.__SIGN_METHOD = 'md5'
        self.__APP_SECRET = app_secret
        self.__SMIPLIFY = True
        self.__V = '2.0'
        self.__URL = 'http://gw.api.taobao.com/router/rest'

    def timestamp(self):
        """

        :return:
        """
        return datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")

    def get_params(self):
        """

        :return:
        """
        params = {
            'app_key': self.__APP_KEY,
            'session': self.__SESSION,
            'timestamp': self.timestamp(),
            'format': self.__FORMAT,
            'v': self.__V,
            'sign_method': self.__SIGN_METHOD,
            'simplify': self.__SMIPLIFY,
        }
        return params

    def make_request(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        params = self.get_params()
        params.update(kwargs)
        sign = make_sign(self.__APP_SECRET, params)
        params['sign'] = sign
        url = self.__URL + '?' + concat_params(params)
        response = requests.post(url).json()
        return response


class Get_Taobao_Seller_Info(TaobaoRequest):
    """

    """

    def __init__(self, app_key, session, app_secret):
        super().__init__(app_key, session, app_secret)
        self.params = {
            'method': 'taobao.user.seller.get',
        }

    def get_response(self, fields):
        """

        :param fields:
        :return:
        """
        self.params['fields'] = fields
        return self.make_request(**self.params)

    # def sink_mysql(self):


class Get_Taobao_Trades_Sold(TaobaoRequest):
    """

    """

    def __init__(self, app_key, session, app_secret):
        super().__init__(app_key, session, app_secret)
        self.params = {
            'method': 'taobao.trades.sold.get',
            'page_size': 100,
            'use_has_next': 'true'
        }
        self.session = DBSession()

    def get_response(self, start_created, end_created, page_no, fields):
        """

        :param start_created:
        :param end_created:
        :param page_no:
        :param fields:
        :return:
        """
        parameters = {
            'start_created': start_created,
            'end_created': end_created,
            'page_no': page_no,
            'fields': fields,
        }
        self.params.update(parameters)
        json_data = self.make_request(**self.params)
        for i in range(len(json_data['trades'])):
            self.order_to_mysql(**json_data['trades'][i])
            for j in range(len(json_data['trades'][i]['orders'])):
                self.order_detail_to_mysql(json_data['trades'][i]['tid'], **json_data['trades'][i]['orders'][j])
        while json_data['has_next']:
            print(f'第{page_no}页数据同步完毕！！！')
            page_no += 1
            time.sleep(10)
            self.get_response(start_created, end_created, page_no, fields)
        self.session.close()
        return '天猫卖家已卖出的交易数据（根据创建时间）同步完成！！！'

    def order_to_mysql(self, **kwargs):
        order = TaoBao_Trade_Order(
            seller_nick=kwargs.get('seller_nick', None),
            pic_path=kwargs.get('pic_path', None),
            payment=kwargs.get('payment', None),
            seller_rate=kwargs.get('seller_rate', None),
            post_fee=kwargs.get('post_fee', None),
            receiver_name=kwargs.get('receiver_name', None),
            receiver_state=kwargs.get('receiver_state', None),
            receiver_zip=kwargs.get('receiver_zip', None),
            receiver_mobile=kwargs.get('receiver_mobile', None),
            receiver_phone=kwargs.get('receiver_phone', None),
            consign_time=kwargs.get('consign_time', None),
            received_payment=kwargs.get('received_payment', None),
            receiver_country=kwargs.get('receiver_country', None),
            receiver_town=kwargs.get('receiver_town', None),
            order_tax_fee=kwargs.get('order_tax_fee', None),
            shop_pick=kwargs.get('shop_pick', None),
            tid=kwargs.get('tid', None),
            num=kwargs.get('num', None),
            num_iid=kwargs.get('num_iid', None),
            status=kwargs.get('status', None),
            title=kwargs.get('title', None),
            tid_type=kwargs.get('tid_type', None),
            price=kwargs.get('price', None),
            discount_fee=kwargs.get('discount_fee', None),
            total_fee=kwargs.get('total_fee', None),
            created=kwargs.get('created', None),
            pay_time=kwargs.get('pay_time', None),
            modified=kwargs.get('modified', None),
            end_time=kwargs.get('end_time', None),
            seller_flag=kwargs.get('seller_flag', None),
            buyer_nick=kwargs.get('buyer_nick', None),
            has_buyer_message=kwargs.get('has_buyer_message', None),
            credit_card_fee=kwargs.get('credit_card_fee', None),
            step_trade_status=kwargs.get('step_trade_status', None),
            step_paid_fee=kwargs.get('step_paid_fee', None),
            mark_desc=kwargs.get('mark_desc', None),
            shipping_type=kwargs.get('shipping_type', None),
            adjust_fee=kwargs.get('adjust_fee', None),
            trade_from=kwargs.get('trade_from', None),
            buyer_rate=kwargs.get('buyer_rate', None),
            receiver_city=kwargs.get('receiver_city', None),
            receiver_district=kwargs.get('receiver_district', None),
            es_range=kwargs.get('es_range', None),
            es_date=kwargs.get('es_date', None),
            os_range=kwargs.get('os_range', None),
            os_date=kwargs.get('os_date', None),
            post_gate_declare=kwargs.get('post_gate_declare', None),
            cross_bonded_declare=kwargs.get('cross_bonded_declare', None),
            order_tax_promotion_fee=kwargs.get('order_tax_promotion_fee', None),
            service_type=kwargs.get('service_type', None),
            is_o2o_passport=kwargs.get('is_o2o_passport', None),
            tmall_delivery=kwargs.get('tmall_delivery', None),
            cn_service=kwargs.get('cn_service', None),
            cutoff_minutes=kwargs.get('cutoff_minutes', None),
            es_time=kwargs.get('es_time', None),
            delivery_time=kwargs.get('delivery_time', None),
            collect_time=kwargs.get('collect_time', None),
            dispatch_time=kwargs.get('dispatch_time', None),
            sign_time=kwargs.get('sign_time', None),
            delivery_cps=kwargs.get('delivery_cps', None),
        )
        self.session.merge(order)
        self.session.commit()

    def order_detail_to_mysql(self, tid, **kwargs):
        order_detail = TaoBao_Trade_Order_Detail(
            tid=tid,
            item_meal_name=kwargs.get('item_meal_name', None),
            pic_path=kwargs.get('pic_path', None),
            seller_nick=kwargs.get('seller_nick', None),
            buyer_nick=kwargs.get('buyer_nick', None),
            refund_status=kwargs.get('refund_status', None),
            outer_iid=kwargs.get('outer_iid', None),
            snapshot_url=kwargs.get('snapshot_url', None),
            snapshot=kwargs.get('snapshot', None),
            timeout_action_time=kwargs.get('timeout_action_time', None),
            buyer_rate=kwargs.get('buyer_rate', None),
            seller_rate=kwargs.get('seller_rate', None),
            seller_type=kwargs.get('seller_type', None),
            cid=kwargs.get('cid', None),
            sub_order_tax_fee=kwargs.get('sub_order_tax_fee', None),
            sub_order_tax_rate=kwargs.get('sub_order_tax_rate', None),
            estimate_con_time=kwargs.get('estimate_con_time', None),
            biz_code=kwargs.get('biz_code', None),
            cloud_store=kwargs.get('cloud_store', None),
            hj_settle_no_commission=kwargs.get('hj_settle_no_commission', None),
            order_taking=kwargs.get('order_taking', None),
            cloud_store_token=kwargs.get('cloud_store_token', None),
            cloud_store_bind_pos=kwargs.get('cloud_store_bind_pos', None),
            os_activity_id=kwargs.get('os_activity_id', None),
            os_fg_item_id=kwargs.get('os_fg_item_id', None),
            os_gift_count=kwargs.get('os_gift_count', None),
            os_sort_num=kwargs.get('os_sort_num', None),
            oid=kwargs.get('oid', None),
            status=kwargs.get('status', None),
            title=kwargs.get('title', None),
            item_oid=kwargs.get('item_oid', None),
            oid_type=kwargs.get('oid_type', None),
            price=kwargs.get('price', None),
            num_iid=kwargs.get('num_iid', None),
            service_id=kwargs.get('service_id', None),
            item_meal_id=kwargs.get('item_meal_id', None),
            sku_id=kwargs.get('sku_id', None),
            num=kwargs.get('num', None),
            outer_sku_id=kwargs.get('outer_sku_id', None),
            order_from=kwargs.get('order_from', None),
            total_fee=kwargs.get('total_fee', None),
            service_detail_url=kwargs.get('service_detail_url', None),
            payment=kwargs.get('payment', None),
            discount_fee=kwargs.get('discount_fee', None),
            adjust_fee=kwargs.get('adjust_fee', None),
            modified=kwargs.get('modified', None),
            sku_properties_name=kwargs.get('sku_properties_name', None),
            refund_id=kwargs.get('refund_id', None),
            is_oversold=kwargs.get('is_oversold', None),
            is_service_order=kwargs.get('is_service_order', None),
            end_time=kwargs.get('end_time', None),
            consign_time=kwargs.get('consign_time', None),
            shipping_type=kwargs.get('shipping_type', None),
            bind_oid=kwargs.get('bind_oid', None),
            logistics_company=kwargs.get('logistics_company', None),
            invoice_no=kwargs.get('invoice_no', None),
            is_daixiao=kwargs.get('is_daixiao', None),
            divide_order_fee=kwargs.get('divide_order_fee', None),
            part_mjz_discount=kwargs.get('part_mjz_discount', None),
            store_code=kwargs.get('store_code', None),
            is_www=kwargs.get('is_www', None),
            bind_oids=kwargs.get('bind_oids', None),
            md_qualification=kwargs.get('md_qualification', None),
            md_fee=kwargs.get('md_fee', None),
            customization=kwargs.get('customization', None),
            fqg_num=kwargs.get('fqg_num', None),
            is_fqg_s_fee=kwargs.get('is_fqg_s_fee', None),
            modify_address=kwargs.get('modify_address', None),
            ti_modify_address_time=kwargs.get('ti_modify_address_time', None),
            es_range=kwargs.get('es_range', None),
            es_date=kwargs.get('es_date', None),
            os_range=kwargs.get('os_range', None),
            os_date=kwargs.get('os_date', None),
            cutoff_minutes=kwargs.get('cutoff_minutes', None),
            es_time=kwargs.get('es_time', None),
            delivery_time=kwargs.get('delivery_time', None),
            collect_time=kwargs.get('collect_time', None),
            dispatch_time=kwargs.get('dispatch_time', None),
            sign_time=kwargs.get('sign_time', None),
            promise_end_time=kwargs.get('promise_end_time', None),
            propoint=kwargs.get('propoint', None),
            is_kaola=kwargs.get('is_kaola', None),
            special_refund_type=kwargs.get('special_refund_type', None),
            extend_info=kwargs.get('extend_info', None),
        )
        self.session.merge(order_detail)
        self.session.commit()
