data=read.csv("D:/硕士期间/Hadoop/cleaned_data2.csv")
View(data)
col=c('back_legroom', 'city_fuel_economy', 'daysonmarket', 'dealer_zip', 'engine_displacement', 'franchise_dealer', 'front_legroom', 'fuel_tank_volume', 'height', 'highway_fuel_economy', 'horsepower', 'is_new', 'latitude', 'length', 'listed_date', 'longitude', 'maximum_seating', 'mileage', 'power', 'price', 'savings_amount', 'seller_rating', 'torque', 'transmission_display', 'wheelbase', 'width', 'year')
names(data)=col
my_lm=lm(price~0+.,data = data)
summary(my_lm)
