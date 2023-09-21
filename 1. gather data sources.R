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


ofcom.mob.pcon2$sumcheck = ofcom.mob.pcon2$`3G_prem_out_0` + ofcom.mob.pcon2$`3G_prem_out_1` + ofcom.mob.pcon2$`3G_prem_out_2` + ofcom.mob.pcon2$`3G_prem_out_3` + ofcom.mob.pcon2$`3G_prem_out_4`

#mae all the NA values 0 as that's what this really means!
ofcom.mob.pcon2[is.na(ofcom.mob.pcon2)] <- 0

#sum together %ge for 0 and 1 providers 
ofcom.mob.pcon2$`4G_prem_out_0_1` <- ofcom.mob.pcon2$`4G_prem_out_0` + ofcom.mob.pcon2$`4G_prem_out_1`
ofcom.mob.pcon2$`3G_prem_out_0_1` <- ofcom.mob.pcon2$`3G_prem_out_0` + ofcom.mob.pcon2$`3G_prem_out_1`
plot(ofcom.mob.pcon2$`4G_prem_out_0_1`, ofcom.mob.pcon2$`3G_prem_out_0_1`, cex = .5, main = "%ge getting 4G coverage from  0 or 1 provider vs 3G from 0 or 1")

plot(ofcom.mob.pcon2$`4G_prem_out_0`, ofcom.mob.pcon2$`4G_prem_out_1`, cex = .5, main = "%ge getting no 4G coverage by %ge 4G from just 1 provider")
plot(ofcom.mob.pcon2$`4G_prem_out_0_1`, ofcom.mob.pcon2$`4G_prem_out_4`, cex = .5, main = "%ge getting 4G coverage from  0 or 1 provider by %ge getting coverage from all 4")

pca.df <- prcomp(ofcom.mob.pcon2[,14:15])
summary(pca.df)
ofcom.mob.pcon2 <- bind_cols(ofcom.mob.pcon2,pca.df$x)
