# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class HousePriceItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    house_type = Field()
    source_unique = Field() #唯一存在 地点 + 小区名字 + 时间+ 钱数 hash
    create_ts = Field()
    updated_ts = Field()
    #new_house
    new_price_id = Field() #新楼盘一个日期楼盘价格id，例如(2018年02月) 1.70万元/㎡
    location = Field()  # {'city':'杭州,'district':余杭区,'street';解放路}
    newest_release_ts = Field() #平均单价时间 时间戳
    newest_release_ti = Field() #平均单价时间 {'year': ,'month': ,'day': }
    house_name = Field() #金成·英特学府
    unit_price = Field() #平均单价
    classification = Field() #分类
    building_type = Field() #建筑类型
    purpose = Field() #用途
    volumetric_rate = Field()  #容积率
    greening_rate = Field() #绿化率
    developers = Field()  # 开发商
    property_address = Field() #物业地址
    area_covered = Field() #占地面积
    build_up_area = Field() # 建筑面积
    property_fee = Field()  #物业费
    property_company = Field() #物业公司
    land_use_years = Field() # 土地使用年限
    opening_ti = Field() #开盘时间 {'year': ,'month': ,'day': }
    opening_ts = Field() # 开盘时间时间戳
    pre_sale_permit = Field() #预售许可证
    last_news = Field() #最新消息
    comprehensive_score = Field() # 综合评分
    new_source_url = Field() # url
    
    #old_house
    old_house_total_price = Field() #改二手房总价
    old_house_unit_price = Field() #二手房单价
    old_house_purpose = Field() #用途
    old_house_area = Field() #房间面积
    old_house_apartment_layout = Field() #户型
    old_house_floor = Field() # 楼层
    old_house_direction = Field() #方向
    old_house_ownership = Field() #权属
    architecture_age = Field() #建筑年代
    degree_of_decoration = Field() #装修程度
    subsidiary_facilities = Field() #附属设施
    old_house_newest_release_ts = Field()  # 平均单价时间 时间戳
    old_house_newest_release_ti = Field()  # 平均单价时间 {'year': ,'month': ,'day': }
    
    
