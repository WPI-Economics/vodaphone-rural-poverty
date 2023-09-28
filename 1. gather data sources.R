library(tidyverse)
library(readxl)
library(aws.s3)


######################################################################
#################################################### READ IN DATA
######################################################################

############################################
############# DEPRIVATION #############
############################################

#IMD19 England deprivation
# Deprivation.pcon <- s3read_using(read_xlsx,
#                                  object = "HoC-IMD-PCON-deprivation19.xlsx",
#                                  bucket = "wpi-vodaphone-rural-poverty",
#                                  sheet = "Data constituencies",
#                                  )
#this one is the CDRC all nations appended file

Deprivation.lsoa <- s3read_using(read_csv,
                                 object = "uk_imd2019.csv",
                                 bucket = "wpi-vodaphone-rural-poverty"
) %>% select(LSOA, Rank)

##### deviation to create a LSOA to PCON lookup
# nspl <- s3read_using(read_csv,
#                      object = "NSPL_FEB_2023_UK.csv",
#                      bucket = "wpi-nspl",
#                      col_select = c("pcon","lsoa11", "ctry" ))
# 
# nspl2 <- unique(nspl) #remove duplicates
# Deprivation.lsoa <- left_join(Deprivation.lsoa, nspl2, by = c("LSOA" = "lsoa11"))
# 
# #NOTE NATIONS ARE NEVER TO BE COMPARED AS THE RANKINGS ARE INDEPENDENT
# Deprivation.pcon <- Deprivation.lsoa %>% group_by(pcon) %>% 
#                     summarise(`Average IMD rank` = mean(Rank, na.rm = T),
#                               Country = first(ctry))
# 
# #add deciles within nations
# Deprivation.pcon <- Deprivation.pcon %>% group_by(Country) %>% 
#   mutate("IMD decile within nation" = ntile(`Average IMD rank`, 10))
# 
# #because nspl is so big saving locally 
# saveRDS(Deprivation.pcon, "Deprivation.pcon.csv")
Deprivation.pcon <- readRDS("Deprivation.pcon.csv")

#add in the PCON names
pcon.names <- s3read_using(read.csv,
                           object = "Westminster Parliamentary Constituency names and codes UK as at 12_14.csv",
                           bucket = "wpi-nspl/Documents",)

Deprivation.pcon <- left_join(Deprivation.pcon, pcon.names, by = c("pcon" = "PCON14CD"))

Deprivation.pcon <- Deprivation.pcon %>% 
  group_by(Country) %>% 
  mutate(`IMD quintile within nation` = ntile(`Average IMD rank`,5))

table(Deprivation.pcon$`IMD quintile within nation`,Deprivation.pcon$`IMD decile within nation`)
############################################
############# URBAN RURAL DATA ################
############################################

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


############################################
############# OFCOM DATA ################
############################################

#Read in from S3
## connected Nations 2022 mobile PCON data https://www.ofcom.org.uk/research-and-data/multi-sector-research/infrastructure-research/connected-nations-2022/data
## about the data pdf https://www.ofcom.org.uk/__data/assets/pdf_file/0027/249462/202209-about-mobile-coverage-local-and-unitary-authority.pdf

# 2023 update data https://www.ofcom.org.uk/research-and-data/multi-sector-research/infrastructure-research/summer-2023
# about the data https://www.ofcom.org.uk/__data/assets/pdf_file/0031/267619/mobile-coverage-local-unitary-authority-5g-202304.pdf

# #CN 2022 data
# ofcom.mob.pcon <- s3read_using(read_csv, # here you tell the s3read_using which read function you are using, and the arguments of the function can follow inside after a comma as you would in the original function)
#                      object = "202209_mobile_pcon_r01.csv", 
#                      bucket = "wpi-vodaphone-rural-poverty"
#                     )

# CN 2023 update data
ofcom.mob.pcon <- s3read_using(read_csv, # here you tell the s3read_using which read function you are using, and the arguments of the function can follow inside after a comma as you would in the original function)
                               object = "202304_mobile_pcon_with_5g_r01.csv", 
                               bucket = "wpi-vodaphone-rural-poverty"
)
#' select the 3g 4g variables we need
#' the thresholds stated by ofcom for service level assessment are
#' 3g outside premises -100dBm voice and -100dBm data 
#' They give %ge of premises that achieve that standard for 0,1,2,3 and 4 
#' VARIABLES NEEDED: 3G_prem_out_0:4
#' 4g outdoor -105dBm voice and -100dBm data
#' VARIABLES NEEDED: 4G_prem_out_0:4


#5g has `high confidence` and `very high confidence` measures based on predictions of different signal strengths. High confidence is more achievable than very high confidence
#we take high confidence
ofcom.mob.pcon2 <- ofcom.mob.pcon %>% select(parl_const, parl_const_name, prem_count,
                                             #contains("3G_prem_out"), 
                                             contains("4g_prem_out"), 
                                             contains("5g_high_confidence_prem")) 

#mae all the NA values 0 as that's what this really means!
ofcom.mob.pcon2[is.na(ofcom.mob.pcon2)] <- 0
#calculate the 1-3 providers by taking (100 - (All providers + 0 providers)
ofcom.mob.pcon2$`4G_prem_1to3` <- round(100 - (ofcom.mob.pcon2$`4G_prem_out_All` + ofcom.mob.pcon2$`4G_prem_out_None`),2)
ofcom.mob.pcon2$`5G_high_confidence_prem_1to3` <- round(100 - (ofcom.mob.pcon2$`5G_high_confidence_prem_out_All` + ofcom.mob.pcon2$`5G_high_confidence_prem_out_None`),2)

#PCA on all the data to produce the first component that we can use as a score of poor service
pcavars <- c(
             #  "4G_prem_out_None",
             # "5G_high_confidence_prem_out_None",
             # "4G_prem_1to3",
             # "5G_high_confidence_prem_1to3",
          #all %ges for both 4 and 5 smerged together
            "4G_prem_out_All",
            "5G_high_confidence_prem_out_All"
  )
pca.df2 <- prcomp(ofcom.mob.pcon2[pcavars]) #3G_prem removed
summary(pca.df2)
ofcom.mob.pcon2 <- bind_cols(ofcom.mob.pcon2, pca.df2$x)



#add Urban rural to the mix
ofcom.mob.pcon2 <- left_join(ofcom.mob.pcon2, urban_rural.pconE, by = c("parl_const" = "pcon"))
#England only
ofcom.mob.pcon2 <- ofcom.mob.pcon2 %>% filter(!is.na(`Broad RUC11`))

#add deprivation vars to the mix
ofcom.mob.pcon2 <- left_join(ofcom.mob.pcon2, Deprivation.pcon, by = c("parl_const" = "pcon"))

#Weighting PC1 by deprivation
ofcom.mob.pcon2 <- ofcom.mob.pcon2 %>% group_by(Country) %>% mutate(IMD_scaled = (scale(`Average IMD rank`)[,1])
)
#pnorm to remove +/- sign
ofcom.mob.pcon2$IMD_scaled <- pnorm(ofcom.mob.pcon2$IMD_scaled*-1) #make sure polarity is correct so higher is more deprived
ofcom.mob.pcon2$PC1 <- pnorm(ofcom.mob.pcon2$PC1) #higher is worse coverage
#multiply them
ofcom.mob.pcon2$index_score <- ofcom.mob.pcon2$IMD_scaled  * ofcom.mob.pcon2$PC1


# 4G 5G service quintiles
ofcom.mob.pcon2$`Premises service quintiles (1 is worst service)` <- ntile(desc(ofcom.mob.pcon2$PC1),5)

# create premises counts for all the percentages for us to use in calculations later

ofcom.mob.pcon2 <- ofcom.mob.pcon2 %>% rowwise() %>% mutate_at(vars(4:11), list(count = ~ ./100* prem_count))


############################################
############# POPULATIONS ################
############################################

pops <- s3read_using(read_csv,
                                  object = "RUC_PCON_2011_EN_LU.csv",
                                  bucket = "wpi-vodaphone-rural-poverty") %>% select(PCON11CD, `Total population 2011`)

ofcom.mob.pcon2 <- left_join(ofcom.mob.pcon2, pops, by = c("parl_const" = "PCON11CD"))



############################################
############# SAVE OUTFILE ################
############################################

saveRDS(ofcom.mob.pcon2, "ofcom constituency df.rds")



# #quick table 
# 
# #by urban rural
# t1 <- ofcom.mob.pcon2 %>% group_by(`Broad RUC11`) %>% summarise("Mean PCA1 score (z-score, high = bad)" = round(mean(PCA1),2),
#                                                                 "Mean % no 3g service" = round(mean(`3G_prem_out_0`),2),
#                                                                 "Mean % no 4g service" = round(mean(`4G_prem_out_0`),2),
#                                                                 "Mean % 1 3g service" = round(mean(`3G_prem_out_1`),2),
#                                                                 "Mean % 1 4g service" = round(mean(`4G_prem_out_1`),2),
#                                                                 "Mean % 2 3g service" = round(mean(`3G_prem_out_2`),2),
#                                                                 "Mean % 2 4g service" = round(mean(`4G_prem_out_2`),2))
# 
# 
# ofcom.mob.pcon2$`IMD rank Quintile (1 is most deprived)` <- ntile(ofcom.mob.pcon2$`IMD rank 2019`,5)
# 
# #by deprivation
# t2 <- ofcom.mob.pcon2 %>% 
#   group_by(`IMD rank Quintile (1 is most deprived)`) %>% summarise("Mean PCA1 score (z-score, high = bad)" = round(mean(PCA1),2),
#                                                                 "Mean % no 3g service" = round(mean(`3G_prem_out_0`),2),
#                                                                 "Mean % no 4g service" = round(mean(`4G_prem_out_0`),2),
#                                                                 "Mean % 1 3g service" = round(mean(`3G_prem_out_1`),2),
#                                                                 "Mean % 1 4g service" = round(mean(`4G_prem_out_1`),2),
#                                                                 "Mean % 2 3g service" = round(mean(`3G_prem_out_2`),2),
#                                                                 "Mean % 2 4g service" = round(mean(`4G_prem_out_2`),2))
# 
# 
# #by deprivation and urban rural
# t3 <- ofcom.mob.pcon2 %>% 
#   group_by(`IMD rank Quintile (1 is most deprived)`,`Broad RUC11`) %>% summarise("Mean PCA1 score (z-score, high = bad)" = round(mean(PCA1),2),
#                                                                                            "Mean % no 3g service" = round(mean(`3G_prem_out_0`),2),
#                                                                                            "Mean % no 4g service" = round(mean(`4G_prem_out_0`),2),
#                                                                                            "Mean % 1 3g service" = round(mean(`3G_prem_out_1`),2),
#                                                                                            "Mean % 1 4g service" = round(mean(`4G_prem_out_1`),2),
#                                                                                            "Mean % 2 3g service" = round(mean(`3G_prem_out_2`),2),
#                                                                                            "Mean % 2 4g service" = round(mean(`4G_prem_out_2`),2))
# 
# 
# 
# t4 <- ofcom.mob.pcon2 %>% 
#   group_by(`Premises service quintiles (1 is worst service)`) %>% summarise("Mean IMD rank /533" = mean(`IMD rank 2019`),
#                                                                                                     "Predominately rural" = sum(`Broad RUC11` == "Predominantly Rural"),
#                                                                                                     "Urban with Significant Rural" = sum(`Broad RUC11` == "Urban with Significant Rural"),
#                                                                                                     "Predominantly Urban" = sum(`Broad RUC11` == "Predominantly Urban"),
#                                                                                                     "Count LSOAs in most deprived decile" = sum(`Number of LSOAs in most deprived decile`))
# 
# 
# #Rural only
# t5 <- ofcom.mob.pcon2 %>% filter(`Broad RUC11` == "Predominantly Rural") %>%  
#   group_by(`Premises service quintiles (1 is worst service)`) %>% summarise("Mean IMD rank /533" = mean(`IMD rank 2019`),
#                                                                                                     "Predominately rural" = sum(`Broad RUC11` == "Predominantly Rural"),
#                                                                                                     # "Urban with Significant Rural" = sum(`Broad RUC11` == "Urban with Significant Rural"),
#                                                                                                     # "Predominantly Urban" = sum(`Broad RUC11` == "Predominantly Urban"),
#                                                                                                     "Count LSOAs in most deprived decile" = sum(`Number of LSOAs in most deprived decile`))
# 
# 
# 
