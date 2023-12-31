#create Urban Rural for the UK

library(tidyverse)
library(sf)
library(aws.s3)


#########################################################
#########################################################
# NSPL data for the urban rurals
#########################################################
#########################################################


#Urban Rural categories
ru11ind.names <- s3read_using(read.csv,
                              object = "Rural Urban (2011) Indicator names and codes GB as at 12_16.csv",
                              bucket = "wpi-nspl/Documents")

#County Unitary names
laua.names <- s3read_using(read.csv,
                              object = "LAU121_ITL321_ITL221_ITL121_UK_LU.csv",
                              bucket = "wpi-nspl/Documents")

unique(ru11ind.names$RU11NM)
#recode into simple Urban and Rural

#The E&W 2011 UR classes pop of 10,000 as Urban and under that as Rural. Following that rule as much as possible.
#scotland classification https://www.gov.scot/publications/scottish-government-urban-rural-classification-2020/pages/3/
#Scotland classes small towns as less than 10,000
#E&W https://www.ons.gov.uk/methodology/geography/geographicalproducts/ruralurbanclassifications/2011ruralurbanclassification

#these are the categories in the NSPL data:

#Urban - "(England/Wales) Urban major conurbation"   
#Urban - "(England/Wales) Urban minor conurbation"                         
#Urban - "(England/Wales) Urban city and town"
#Urban - "(England/Wales) Urban city and town in a sparse setting"       
#Rural - "(England/Wales) Rural town and fringe"
#Rural - "(England/Wales) Rural town and fringe in a sparse setting"         
#Rural - "(England/Wales) Rural village"
#Rural - "(England/Wales) Rural village in a sparse setting"
#Rural - "(England/Wales) Rural hamlet and isolated dwellings"
#Rural - "(England/Wales) Rural hamlet and isolated dwellings in a sparse setting"
#Urban - "(Scotland) Large Urban Area"
#Urban - "(Scotland) Other Urban Area"     
#Rural - "(Scotland) Accessible Small Town"
#Rural - "(Scotland) Remote Small Town"
#Rural - "(Scotland) Very Remote Small Town"
#Rural - "(Scotland) Accessible Rural"
#Rural - "(Scotland) Remote Rural"                                                 
#Rural - "(Scotland) Very Remote Rural"
#NA -   "(pseudo) Channel Islands/Isle of Man" 

ru11ind.names <- ru11ind.names %>% mutate("Urban_rural" = 
                                    case_when(str_detect(RU11NM, "Urban") ~ "Urban",
                                              str_detect(RU11NM, "Rural|Small Town") ~ "Rural",
                                              .default = NA)
) 


#lookup from lsoa11 to combined authority from ONS geography
# county.look <- sf::st_read(
#   "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LSOA11_UTLA21_EW_LU_16dd2caef0d4425b94e72dc7e4add26f/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
# ) %>% sf::st_drop_geometry()

#lookup from LAD20 to county20
LAD20 <- "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD20_CTY20_EN_LU_598927d71aca422f893bab3ee1283f87/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
LAD19 <- "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD19_CTY19_EN_LUv1_0d42f8f0668f425fba79989263ea08a6/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
LAD21 <- "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD21_CTY21_EN_LU_34f63ba938f34b689b245b7c7c18bde5/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
LAD22 <- "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD22_CTY22_EN_LU/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
LAD23 <- "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LAD23_CTY23_EN_LU/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
LAtoLAUA23 <- "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LTLA23_UTLA23_EW_LU/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"
LAtoLAUA21 <- "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/LTLA21_UTLA21_EW_LU_9bbac05558b74a88bda913ad5bf66917/FeatureServer/0/query?outFields=*&where=1%3D1&f=geojson"


county.look <- sf::st_read(
  LAtoLAUA21,
)%>% sf::st_drop_geometry()

##### UR postcodes from NSPL (inc. Scotland and Wales)
nsplraw <- s3read_using(read_csv,
                     object = "NSPL_MAY_2023_UK.csv",
                     bucket = "wpi-nspl")

nspl <- nsplraw %>% select(c("pcd","lsoa11","pcon", "ru11ind", "lat", "long", "laua" , "ctry"))

#add the UR definitions
nspl <- left_join(nspl, ru11ind.names, by = c("ru11ind" = "RU11IND"))
nspl <- nspl %>% na.omit()


#add Counties
#nspl <- left_join(nspl, county.look, by = c("lsoa11" = "LSOA11CD")) #lsoa version gives duplicates across shared boundaries yuck
nspl <- left_join(nspl, county.look, by = c("laua" = "LTLA21CD")) 

#work out Upper tier from %ge of postcode in a pcon
pconcount <- nspl %>% group_by(pcon, UTLA21NM) %>% 
  summarise("count_UTLA" = n()) %>% pivot_wider(id_cols = pcon, names_from = UTLA21NM, values_from = "count_UTLA")
pconcount[is.na(pconcount)] <- 0

#select the col that has the highest count
pconcount <- pconcount %>%
  rowwise %>%
  mutate(UTLA21NM = names(.[2:176])[which.max(c_across(1:175))]) %>%
  ungroup

pcon_to_UTLA <- pconcount %>% select(pcon,UTLA21NM ) %>% unique()


#########################################################
#########################################################
# now lets grab the new 2023 constituency spatial files, and we will place each postcode centroid in it's parent pcon to get the lookup. Find the files in the AWS
#########################################################
#########################################################

england.sp <- s3read_using(st_read,
                           object = "2023_06_27_Final_recommendations_England.gpkg",
                           bucket =  "wpi-vodaphone-rural-poverty/new-2023-constituencies")

#shapefuiles are a pain and need to see the other files that in the foler manually down loaded into "\shapefiles"
# wales.sp <- s3read_using(st_read,
#                          object = "Final Recommendations_region.shp",
#                          bucket =  "wpi-vodaphone-rural-poverty/new-2023-constituencies/wales-2023")

wales.sp <- st_read("shapefiles/Final Recommendations_region.shp")
scot.sp <- st_read("shapefiles/All_Scotland_Final_Recommended_Constituencies_2023_Review.shp")

#keep just the constituency names and the geom

england.sp <- england.sp %>% select(name = Constituen, geometry = geom)
wales.sp <- wales.sp %>% select(name = Official_N)
scot.sp <- scot.sp %>% select(name = NAME)


england.sp <- sf::st_transform(england.sp, crs = st_crs(4326))
wales.sp <- sf::st_transform(wales.sp, crs = st_crs(4326))
scot.sp <- sf::st_transform(scot.sp, crs = st_crs(4326))

gb.pcon.sp <- reduce(list(england.sp, wales.sp, scot.sp), bind_rows)

plot(gb.pcon.sp)

#########################################################
#########################################################
# now we have to position the postcode centroids in the new pcons
#########################################################
#########################################################

#make nspl spatial
nspl.sp <- st_as_sf(nspl, coords = c("long", "lat"), crs = 4326)

# test <- sample_frac(nspl, .1)
# nspl.sp <- st_as_sf(test, coords = c("long", "lat"), crs = 4326)


sf_use_s2(FALSE) #always causes problems so set to false
int <- sf::st_intersects(nspl.sp, gb.pcon.sp, sparse = T) #int is an index of rownumbers that tell us which pcon bounds have the centroids inside 
#make sure empty items have a value
int[lengths(int) == 0] <- NA

#there are 2 postcodes that get entered twice - presume the points are equal distant from 2 pcons so get added in twice?
which(lengths(int) == 2)
wrong.sp <- nspl.sp[which(lengths(int) == 2),]
removeme <- which(lengths(int) == 2) #row numbers of the offending items


int2 <- sapply(int,"[[",1) #just 1st level of results
int2[removeme] #check they are gone

nspl.sp$pcon.intersect.nm <- as.character(gb.pcon.sp$name[unlist(int2)]) #this unlists the resulting list of rownumbers and selects the pcon codes that correspont, saving them as a new variable in the bounds file


#save up the lookup
lookup <- tibble(pcd = nspl.sp$pcd,
                 pcon23.nm = nspl.sp$pcon.intersect.nm)

#########################################################
#########################################################
# write the lookup to AWS
#########################################################
#########################################################


s3write_using(lookup,
              FUN = write_csv,
              bucket = "wpi-vodaphone-rural-poverty/new-2023-constituencies", # Make sure the correct bucket is being used!
              object = "postcode_to_2023_constituency.csv") # this is the file name


#########################################################
#########################################################
# now add the new pcon variable to the NSPL file
#########################################################
#########################################################

nspl <- left_join(nspl, lookup, by = "pcd")
nspl <- nspl %>% select(-UTLA21CD, -UTLA21NM) #remove UTLA that generates duplicates
nspl <- left_join(nspl, pcon_to_UTLA, by = "pcon") #join the max method one
nspl$UTLA21NM[nspl$UTLA21NM == "NA"] <- NA

s3write_using(nspl,
              FUN = write_csv,
              bucket = "wpi-vodaphone-rural-poverty/new-2023-constituencies", # Make sure the correct bucket is being used!
              object = "nspl_pcon23.csv") # this is the file name
