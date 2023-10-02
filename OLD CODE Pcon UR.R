#urban rural using pre-fab constituency data 


#Urban Rural ENGLAND
urban_rural.pconE <- s3read_using(read_csv,
                                  object = "RUC_PCON_2011_EN_LU.csv",
                                  bucket = "wpi-vodaphone-rural-poverty") %>% 
  select("pcon" = PCON11CD, 
         "Constituency" = PCON11NM, 
         `Broad RUC11`)



#urban rural scotland
urban_rural.pconS <- s3read_using(read_excel,
                                  object = "scottish-urban-rural-classification.xlsx",
                                  bucket = "wpi-vodaphone-rural-poverty",
                                  sheet = "UKPC6FOLD",
                                  skip = 2)
#take max category and classify pcon by largest type of area represented
urban_rural.pconS <- urban_rural.pconS %>% rowwise() %>% 
  mutate(maxval = max(c_across(2:7)),
         name = names(.[2:7])[which.max(c_across(2:7))]
  )

urban_rural.pconS <- urban_rural.pconS %>% select(Constituency, "Broad RUC11" = name)

#match the codes in grrr.
urban_rural.pconS <- left_join(urban_rural.pconS, Deprivation.pcon[,c("pcon", "PCON14NM")] , 
                               by = c("Constituency" = "PCON14NM" )) %>% 
  na.omit()

#and add together England and Scotland
urban_rural.pcon <- bind_rows(urban_rural.pconE, urban_rural.pconS)
# rm(urban_rural.pconE, urban_rural.pconS)

#Wales have gone rogue and we don't have PCON U/R data so will have to build it from the LSOA dataset.
wales.lsoa.UR <- s3read_using(read_csv,
                              object = "Rural_Urban_Classification_(2011)_of_Lower_Layer_Super_Output_Areas_in_England_and_Wales.csv",
                              bucket = "wpi-vodaphone-rural-poverty")
wales.lsoa.UR <- wales.lsoa.UR %>% filter(str_detect(LSOA11CD, "W"))

#now match in PCONs need lookup
# nspl <- s3read_using(read_csv,
#                      object = "NSPL_FEB_2023_UK.csv",
#                      bucket = "wpi-nspl",
#                      col_select = c("pcon","lsoa11", "ctry" ))
# 
# nspl <- unique(nspl) #remove duplicates
# #save the lookup
# saveRDS(nspl, "lsoa_pcon_lookup.RDS")
lsoa.to.pcon <- readRDS("lsoa_pcon_lookup.RDS")

wales.lsoa.UR <- left_join(wales.lsoa.UR, lsoa.to.pcon, by = c("LSOA11CD" = "lsoa11"))
#summarise count each UR category within PCON
table(wales.lsoa.UR$RUC11)

#here is the ONS methods paper for LAD UR classification https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/591465/RUCLAD2011_User_Guide__January_2017_.pdf

####These are the U/R categories used at the LSOA level
# "Rural town and fringe"                             <- Rural
# "Rural town and fringe in a sparse setting"         <- Rural     
# "Rural village and dispersed"                       <- Rural
# "Rural village and dispersed in a sparse setting"   <- Rural
# "Urban city and town"                               <- Urban
# "Urban city and town in a sparse setting"           <- Urban

####And these are what we get at PCON level
# "Accessible rural"                                  <- Rural     
# "Accessible small towns"                            <- Urban
# "Large urban"                                       <- Urban         
# "Other urban"                                       <- Urban
# "Predominantly Rural"                               <- Rural
# "Predominantly Urban"                               <- Urban
# "Remote rural"                                      <- Rural
# "Urban with Significant Rural"                      <- Urban with sig Rural





# wales.pcon.UR <- wales.lsoa.UR %>% group_by(pcon) %>% 
#   summarise("Rural town and fringe"   
#             "Rural town and fringe in a sparse setting"         
#             "Rural village and dispersed" 
#             "Rural village and dispersed in a sparse setting"   
#             "Urban city and town"  
#             "Urban city and town in a sparse setting")


########################################################################
#'Up to this point work to do to bring in U?R for UK in a harmonised way 
#' coming back to it
########################################################################