library(tidyverse)
library(readxl)
library(aws.s3)


##########################
######## READ IN DATA
##########################

#Read in from S3
## connected Nations 2022 mobile PCON data https://www.ofcom.org.uk/research-and-data/multi-sector-research/infrastructure-research/connected-nations-2022/data
## about the data pdf https://www.ofcom.org.uk/__data/assets/pdf_file/0027/249462/202209-about-mobile-coverage-local-and-unitary-authority.pdf

ofcom.mob.pcon <- s3read_using(read_csv, # here you tell the s3read_using which read function you are using, and the arguments of the function can follow inside after a comma as you would in the original function)
                     object = "202209_mobile_pcon_r01.csv", 
                     bucket = "wpi-vodaphone-rural-poverty"
                    )

#keep England only
ofcom.mob.pcon <- ofcom.mob.pcon %>% filter(str_detect(parl_const, "E"))


Deprivation.pcon <- s3read_using(read_xlsx,
                                 object = "HoC-IMD-PCON-deprivation19.xlsx",
                                 bucket = "wpi-vodaphone-rural-poverty",
                                 sheet = "Data constituencies",
                                 )


urban_rural.pcon <- s3read_using(read_csv,
                                 object = "RUC_PCON_2011_EN_LU.csv",
                                 bucket = "wpi-vodaphone-rural-poverty")



#' select the 3g 4g variables we need
#' the thresholds stated by ofcom for service level assessment are
#' 3g outside premises -100dBm voice and -100dBm data 
#' They give %ge of premises that achieve that standard for 0,1,2,3 and 4 
#' VARIABLES NEEDED: 3G_prem_out_0:4
#' 4g outdoor -105dBm voice and -100dBm data
#' VARIABLES NEEDED: 4G_prem_out_0:4

ofcom.mob.pcon2 <- ofcom.mob.pcon %>% select(parl_const, parl_const_name, contains("3G_prem_out"), contains("4g_prem_out"))

#mae all the NA values 0 as that's what this really means!
ofcom.mob.pcon2[is.na(ofcom.mob.pcon2)] <- 0

#sum together %ge for 0 and 1 providers 
# ofcom.mob.pcon2$`4G_prem_out_0_1` <- ofcom.mob.pcon2$`4G_prem_out_0` + ofcom.mob.pcon2$`4G_prem_out_1`
# ofcom.mob.pcon2$`3G_prem_out_0_1` <- ofcom.mob.pcon2$`3G_prem_out_0` + ofcom.mob.pcon2$`3G_prem_out_1`
# plot(ofcom.mob.pcon2$`4G_prem_out_0_1`, ofcom.mob.pcon2$`3G_prem_out_0_1`, cex = .5, main = "%ge getting 4G coverage from  0 or 1 provider vs 3G from 0 or 1")
# 
# plot(ofcom.mob.pcon2$`4G_prem_out_0`, ofcom.mob.pcon2$`4G_prem_out_1`, cex = .5, main = "%ge getting no 4G coverage by %ge 4G from just 1 provider")
# plot(ofcom.mob.pcon2$`4G_prem_out_0_1`, ofcom.mob.pcon2$`4G_prem_out_4`, cex = .5, main = "%ge getting 4G coverage from  0 or 1 provider by %ge getting coverage from all 4")

#A PCA against the 4G and 3G 0 and 1 provider vars to summarise them
# pca.df <- prcomp(ofcom.mob.pcon2[,14:15])
# summary(pca.df)
# ofcom.mob.pcon3 <- bind_cols(ofcom.mob.pcon2,pca.df$x)

#PCA on all the data to produce the first component that we can use as a score of poor service
pca.df2 <- prcomp(ofcom.mob.pcon2[,str_detect(colnames(ofcom.mob.pcon2), "3G_prem|4G_prem")])
summary(pca.df2)
ofcom.mob.pcon2 <- bind_cols(ofcom.mob.pcon2, pca.df2$x[,1])
colnames(ofcom.mob.pcon2)[colnames(ofcom.mob.pcon2) == "...13"] <- "PCA1"

#add Urban rural to the mix
ofcom.mob.pcon2 <- left_join(ofcom.mob.pcon2, urban_rural.pcon[,c(1,14)], by = c("parl_const" = "PCON11CD"))

#add deprivation vars to the mix
ofcom.mob.pcon2 <- left_join(ofcom.mob.pcon2, Deprivation.pcon, by = c("parl_const" = "ONSConstID"))

#3G 4G service quintiles
ofcom.mob.pcon2$`Premises service quintiles (1 is worst service)` <- ntile(desc(ofcom.mob.pcon2$PCA1),5)


#quick table 

#by urban rural
t1 <- ofcom.mob.pcon2 %>% group_by(`Broad RUC11`) %>% summarise("Mean PCA1 score (z-score, high = bad)" = round(mean(PCA1),2),
                                                                "Mean % no 3g service" = round(mean(`3G_prem_out_0`),2),
                                                                "Mean % no 4g service" = round(mean(`4G_prem_out_0`),2),
                                                                "Mean % 1 3g service" = round(mean(`3G_prem_out_1`),2),
                                                                "Mean % 1 4g service" = round(mean(`4G_prem_out_1`),2),
                                                                "Mean % 2 3g service" = round(mean(`3G_prem_out_2`),2),
                                                                "Mean % 2 4g service" = round(mean(`4G_prem_out_2`),2))


ofcom.mob.pcon2$`IMD rank Quintile (1 is most deprived)` <- ntile(ofcom.mob.pcon2$`IMD rank 2019`,5)

#by deprivation
t2 <- ofcom.mob.pcon2 %>% 
  group_by(`IMD rank Quintile (1 is most deprived)`) %>% summarise("Mean PCA1 score (z-score, high = bad)" = round(mean(PCA1),2),
                                                                "Mean % no 3g service" = round(mean(`3G_prem_out_0`),2),
                                                                "Mean % no 4g service" = round(mean(`4G_prem_out_0`),2),
                                                                "Mean % 1 3g service" = round(mean(`3G_prem_out_1`),2),
                                                                "Mean % 1 4g service" = round(mean(`4G_prem_out_1`),2),
                                                                "Mean % 2 3g service" = round(mean(`3G_prem_out_2`),2),
                                                                "Mean % 2 4g service" = round(mean(`4G_prem_out_2`),2))


#by deprivation and urban rural
t3 <- ofcom.mob.pcon2 %>% 
  group_by(`IMD rank Quintile (1 is most deprived)`,`Broad RUC11`) %>% summarise("Mean PCA1 score (z-score, high = bad)" = round(mean(PCA1),2),
                                                                                           "Mean % no 3g service" = round(mean(`3G_prem_out_0`),2),
                                                                                           "Mean % no 4g service" = round(mean(`4G_prem_out_0`),2),
                                                                                           "Mean % 1 3g service" = round(mean(`3G_prem_out_1`),2),
                                                                                           "Mean % 1 4g service" = round(mean(`4G_prem_out_1`),2),
                                                                                           "Mean % 2 3g service" = round(mean(`3G_prem_out_2`),2),
                                                                                           "Mean % 2 4g service" = round(mean(`4G_prem_out_2`),2))



t4 <- ofcom.mob.pcon2 %>% 
  group_by(`Premises service quintiles (1 is worst service)`) %>% summarise("Mean IMD rank /533" = mean(`IMD rank 2019`),
                                                                                                    "Predominately rural" = sum(`Broad RUC11` == "Predominantly Rural"),
                                                                                                    "Urban with Significant Rural" = sum(`Broad RUC11` == "Urban with Significant Rural"),
                                                                                                    "Predominantly Urban" = sum(`Broad RUC11` == "Predominantly Urban"),
                                                                                                    "Count LSOAs in most deprived decile" = sum(`Number of LSOAs in most deprived decile`))


#Rural only
t5 <- ofcom.mob.pcon2 %>% filter(`Broad RUC11` == "Predominantly Rural") %>%  
  group_by(`Premises service quintiles (1 is worst service)`) %>% summarise("Mean IMD rank /533" = mean(`IMD rank 2019`),
                                                                                                    "Predominately rural" = sum(`Broad RUC11` == "Predominantly Rural"),
                                                                                                    # "Urban with Significant Rural" = sum(`Broad RUC11` == "Urban with Significant Rural"),
                                                                                                    # "Predominantly Urban" = sum(`Broad RUC11` == "Predominantly Urban"),
                                                                                                    "Count LSOAs in most deprived decile" = sum(`Number of LSOAs in most deprived decile`))



