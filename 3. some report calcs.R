library(tidyverse)

#Report extras

#% constituencies rural and in the 40% most deprived
ofcom.df <- ofcom.df %>% ungroup() %>%  
  mutate("Poor coverage percentile (1 is poor)" = ntile(`PC1`,100),
         "Poor coverage x Deprivation percentile (1 is poor)" = ntile(index_score,100)) 


  
rural.40 <- ofcom.df %>% filter(Rural == "Predominantly Rural" & `Deprivation group` == "1 & 2 Most deprived 40%") %>% nrow()
rural.40.poor <- ofcom.df %>% filter(Rural == "Predominantly Rural" & `Deprivation group` == "1 & 2 Most deprived 40%" & 
                                       `Poor coverage percentile (1 is poor)` <= 20) %>% nrow()

round(rural.40.poor/rural.40*100,1)

#number of people living in the rural.40.poor group
rural.40.poor.df <- ofcom.df %>% filter(Rural == "Predominantly Rural" & `Deprivation group` == "1 & 2 Most deprived 40%" & 
                                       `Poor coverage percentile (1 is poor)` <= 20) %>% 
  
rural.40.poor.pop21 <- rural.40.poor.df %>% summarise(x = sum(`Total population 2021`))




u.40 <- ofcom.df %>% filter(Rural == "Predominantly Urban" & `Deprivation group` == "1 & 2 Most deprived 40%") %>% nrow()
u.40.poor <- ofcom.df %>% filter(Rural == "Predominantly Urban" & `Deprivation group` == "1 & 2 Most deprived 40%" & 
                                       `Poor coverage percentile (1 is poor)` <= 20) %>% nrow()

round(u.40.poor/u.40*100,1)




#p5 
# Cases of higher deprivation and lower connectivity levels coinciding are far more common in rural constituencies: of the 20% worst performing constituencies on the index, 53 are rural.

all.40.poor <-  ofcom.df %>% filter(`Poor coverage percentile (1 is poor)` <= 20) %>% nrow()
rural.40 / all.40.poor *100


#table of deprivation quintile tally in the poorest 20% of the index
pcon.table$Rural_Urban_strict <- ifelse(pcon.table$`Rural/Urban group` %in% c("Predominantly Rural", "Urban with Significant Rural"), "Rural (both)", "Urban" )
table(pcon.table$Rural_Urban_strict, pcon.table$`Rural/Urban group`)


deptally20_pc <- pcon.table %>% filter(`Coverage / Deprivation index percentile (1 is worst)` <=20) %>% 
  group_by(`Rural_Urban_strict`,`Deprivation quintile 1 is lowest`) %>% summarise(Count = n())
