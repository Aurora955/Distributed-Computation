#!/usr/bin/env python3
import csv
import sys

col_num = 66
line_num = 0
na_count = [0]*col_num
col_sum = [0]*col_num

# 筛选带有单位的变量索引，方便后续对数值型变量求和
col_name = ['vin', 'back_legroom', 'bed', 'bed_height', 'bed_length', 'body_type', 'cabin', 'city', 'city_fuel_economy', 'combine_fuel_economy', 'daysonmarket', 'dealer_zip', 'description', 'engine_cylinders', 'engine_displacement', 'engine_type', 'exterior_color', 'fleet', 'frame_damaged', 'franchise_dealer', 'franchise_make', 'front_legroom', 'fuel_tank_volume', 'fuel_type', 'has_accidents', 'height', 'highway_fuel_economy', 'horsepower', 'interior_color', 'isCab', 'is_certified', 'is_cpo', 'is_new', 'is_oemcpo', 'latitude', 'length', 'listed_date', 'listing_color', 'listing_id', 'longitude', 'main_picture_url', 'major_options', 'make_name', 'maximum_seating', 'mileage', 'model_name', 'owner_count', 'power', 'price', 'salvage', 'savings_amount', 'seller_rating', 'sp_id', 'sp_name', 'theft_title', 'torque', 'transmission', 'transmission_display', 'trimId', 'trim_name', 'vehicle_damage_category', 'wheel_system', 'wheel_system_display', 'wheelbase', 'width', 'year']
in_name = ("back_legroom","front_legroom","height","length","wheelbase","width")
in_index = []
for a in in_name:
    in_index.append(col_name.index(a))
gal_index = col_name.index("fuel_tank_volume")
seats_index = col_name.index("maximum_seating")
#print(in_index,gal_index,seats_index)

# 读入数据
reader = csv.reader(sys.stdin)
# 跳过首行
next(reader)
for line in reader:
    try:
        # 计算行总数
        line_num = line_num+1
        #print(line)       
        for i in range(col_num):
            # 剔除数值型变量的单位
            line[i].strip()
            if i in in_index:
                line[i] = line[i].strip("in").strip()
            if i == gal_index:
                line[i] = line[i].strip("gal").strip()
            if i == seats_index:
                line[i] = line[i].strip("seats").strip()
            # 将字符型变量转化成数值型
            try:
                line[i] = int(line[i])
            except(ValueError):
                try:
                    line[i] = float(line[i]) 
                except:
                    pass
	    #except:
                #pass   
            # 对每一列数值型变量求和
            try:  
                col_sum[i] = col_sum[i]+line[i]
            except:
                pass
            # 计算每一列缺失值的个数
            if line[i]=="" or line[i]=="--":
                na_count[i] = na_count[i]+1
        # print(line)
        na_count.append(line_num)
        #print(len(na_count),len(col_sum))
        print(",".join('%s' %id for id in na_count))
        print(",".join('%s' %id for id in col_sum))
    except:
        pass
