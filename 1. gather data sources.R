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
                           bucket = "wpi-nspl/Documents")

Deprivation.pcon <- left_join(Deprivation.pcon, pcon.names, by = c("pcon" = "PCON14CD"))

Deprivation.pcon <- Deprivation.pcon %>% 
  group_by(Country) %>% 
  mutate(`IMD quintile within nation` = ntile(`Average IMD rank`,5))

table(Deprivation.pcon$`IMD quintile within nation`,Deprivation.pcon$`IMD decile within nation`)
############################################
############# URBAN RURAL DATA ################
############################################

#read in nspl data created in file 0.1
pcds <- s3read_using(read_csv,
                     object = "nspl_pcon23.csv",
                     bucket = "wpi-vodaphone-rural-poverty/new-2023-constituencies")

#add in the pcon names
pcds <- left_join(pcds, pcon.names, by = c("pcon" = "PCON14CD"))

#remove the 2023 names
pcds <- pcds %>% select(-pcon23.nm)

#create urban rural for pcons
pcon.UR <- pcds %>% group_by(pcon, PCON14NM) %>% 
  summarise(`Rural_count`  = length(pcd[Urban_rural == "Rural"]),
            `Urban_count` = length(pcd[Urban_rural == "Urban"]),
            `All_pcds_count` = length(pcd))

pcon.UR <- pcon.UR %>% mutate(
  Pct_Rural = (Rural_count / All_pcds_count) *100,
  Pct_Urban = (Urban_count / All_pcds_count) *100
)


#'trying to stick a bit close to the ONS LAD rules
#'mainly and largely rural are 50% rural or more
#'urban with sig rural is 26-49% rural - ONS look for hub towns which we can't do here
#'rest urban

pcon.UR <- pcon.UR %>% mutate(
 Rural = case_when(
   Pct_Rural >= 50 ~ "Predominately rural",
   Pct_Rural >= 26 & Pct_Rural < 50 ~ "Urban with significant rural",
   Pct_Rural <26 ~ "Predominately urban"
 )
)

#check agains ONS
#Urban Rural ENGLAND
urban_rural.pconE <- s3read_using(read_csv,
                                  object = "RUC_PCON_2011_EN_LU.csv",
                                  bucket = "wpi-vodaphone-rural-poverty") %>% 
  select("pcon" = PCON11CD, 
         "Constituency" = PCON11NM, 
         `Broad RUC11`)

pcon.UR <- left_join(pcon.UR, urban_rural.pconE, by = "pcon")
table(pcon.UR$`Broad RUC11`, pcon.UR$Rural)

#keep the ONS data for England, and use this new variable for Wales and Scotland
pcon.UR <- pcon.UR %>% mutate(
  `Urban/Rural GB` = case_when(
    str_starts(PCON14NM, "E") ~ `Broad RUC11`,
    TRUE ~ `Rural`,
    
  )
)

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
ofcom.mob.pcon2$`4G_prem_1to3` <- round(100 - (ofcom.mob.pcon2$`4G_prem_out_All` + 
                                                 ofcom.mob.pcon2$`4G_prem_out_None`),2)

ofcom.mob.pcon2$`5G_high_confidence_prem_1to3` <- round(100 - (ofcom.mob.pcon2$`5G_high_confidence_prem_out_All` + 
                                                                 ofcom.mob.pcon2$`5G_high_confidence_prem_out_None`),2)


#sumchecks
ofcom.mob.pcon2$test <- ofcom.mob.pcon2$`5G_high_confidence_prem_1to3`+ 
  ofcom.mob.pcon2$`5G_high_confidence_prem_out_None` + 
  ofcom.mob.pcon2$`5G_high_confidence_prem_out_All`

ofcom.mob.pcon2$test

ofcom.mob.pcon2$test <- NULL


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
ofcom.mob.pcon2 <- left_join(ofcom.mob.pcon2, pcon.UR, by = c("parl_const" = "pcon"))

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

ofcom.mob.pcon2$sumcheck <- ofcom.mob.pcon2$`5G_high_confidence_prem_1to3_count` + 
                            ofcom.mob.pcon2$`5G_high_confidence_prem_out_All_count` +
                            ofcom.mob.pcon2$`5G_high_confidence_prem_out_None_count`

round(ofcom.mob.pcon2$sumcheck - ofcom.mob.pcon2$prem_count,2)

############################################
############# POPULATIONS ################
############################################

pops <- s3read_using(read_csv,
                                  object = "RUC_PCON_2011_EN_LU.csv",
                                  bucket = "wpi-vodaphone-rural-poverty") %>% select(PCON11CD, `Total population 2011`)




scotpops <- s3read_using(read_excel,
                         object = "Scotlandpops_ukpc-21-tabs.xlsx",
                         bucket = "wpi-vodaphone-rural-poverty",
                         sheet = "2021",
                         skip = 3) %>% filter(Sex == "Persons", ) %>% select(c(2,4))

scotpops <- scotpops %>% rename(PCON11CD = `UK Parliamentary Constituency 2005 Code`,
                                "Total population 2021" = Total)


Eng_walespops <- s3read_using(
  read_csv,
  object = "Census21 pops.csv",
  bucket = "wpi-vodaphone-rural-poverty",
  skip = 7
  ) %>% na.omit()


Eng_walespops <- Eng_walespops %>% rename(
  PCON11CD = mnemonic,
  "Total population 2021" = `2021`
) %>% select(-`parliamentary constituency 2010`)

pops_all <- bind_rows(Eng_walespops, scotpops)

ofcom.mob.pcon2 <- left_join(ofcom.mob.pcon2, pops_all, by = c("parl_const" = "PCON11CD"))
  
#remove NI
ofcom.mob.pcon2 <- ofcom.mob.pcon2 %>% na.omit(`Total population 2021`)

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
