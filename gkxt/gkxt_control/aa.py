#-*-coding:utf-8 -*-
import json
data='{"card_num": "460007623754041", "idcard": "452626199507101007", "person_type": "\u6d89\u6bd2", "device_province": "", "device_district": "", "device_city": "", "pic1": "", "pic2": "", "pic3": "", "city_code": "", "wd": "23.125374", "device_town": "", "device_street": "", "district_code": "", "jd": "106.303321", "device_id": "7760926", "province_code": "", "col_time": "2019-06-01 04:01:08", "data_source": "\u7535\u5b50\u56f4\u680f", "name": "\u674e\u6625\u6885", "device_name": "7760926_\u767e\u8272\u5e02\u9756\u897f\u53bf\u7984\u6850\u9ad8\u901f\u51fa\u5165\u53e3\u5361\u53e3", "card_type": "3", "etl_time": "2019-06-01 04:05:09", "behavior": "9"}'

def aa():
	    data='{"card_num": "460007623754041", "idcard": "452626199507101007", "person_type": "\u6d89\u6bd2", "device_province": "", "device_district": "", "device_city": "", "pic1": "", "pic2": "", "pic3": "", "city_code": "", "wd": "23.125374", "device_town": "", "device_street": "", "district_code": "", "jd": "106.303321", "device_id": "7760926", "province_code": "", "col_time": "2019-06-01 04:01:08", "data_source": "\u7535\u5b50\u56f4\u680f", "name": "\u674e\u6625\u6885", "device_name": "7760926_\u767e\u8272\u5e02\u9756\u897f\u53bf\u7984\u6850\u9ad8\u901f\u51fa\u5165\u53e3\u5361\u53e3", "card_type": "3", "etl_time": "2019-06-01 04:05:09", "behavior": "9"}'
            data = json.loads(str(data))
            device_district = fieldValue(data["device_district"])
            col_time = str(data["col_time"])
            card_num = str(data["card_num"])
            name = fieldValue(data["name"])
            #device_location = str(data["device_location"])
            idcard = str(data["idcard"])
            pic1 = str(data["pic1"])
            pic2 = str(data["pic2"])
            device_name = fieldValue(data["device_name"])
            person_type = fieldValue(data["person_type"])
            card_type = str(data["card_type"])
            etl_time = str(data["etl_time"])
            data_source = fieldValue(data["data_source"])
            device_province = fieldValue(data["device_province"])
            behavior = fieldValue(data["behavior"])
            device_street = fieldValue(data["device_street"])
            device_city = fieldValue(data["device_city"])
            pic3 = str(data["pic3"])
            device_town = fieldValue(data["device_town"])
            device_id = str(data["device_id"])
            province_code = str(data["province_code"])
            city_code = str(data["city_code"])
            district_code = str(data["district_code"])
            jd = str(data["jd"])
            wd = str(data["wd"])

#判断字段值是否为空
def fieldValue(field_data):
        if field_data is None:
            return ''
        else:
            return field_data

if __name__=='__main__':
	aa()
