library(SparkR)
library(sparklyr)
library(readr)
library(tidyverse)
library(DBI)
library(readr)
library(ggplot2)
library(geobr)
library(rgdal)
library(dplyr)
library(RColorBrewer)
library(leaflet)
library(readxl)
library(scales)
library(GGally)
options(scipen = 999)
Sys.setenv(SPARK_HOME='/opt/spark')
Sys.setenv(SPARK_HOME_VERSION='3.3.0')
Sys.setenv(YARN_CONF_DIR='/opt/hadoop/etc/hadoop')
Sys.setenv(JAVA_HOME = '/usr/lib/jvm/java-14-openjdk-amd64')

# inicia a sessão do spark
sc = spark_connect(master='local')
spark_web(sc)

setwd("/home/ucalvi/Downloads/atividade-3")
################################################################
# EIdentificação dos dados necessários
################################################################
# Dados disponíveis para o problema
df_energia <- read_excel("energia.xlsx")
df_pib <- read_excel("pib.xlsx")
# Dados de apoio
df_municipios <- readOGR("BR", "BR_Municipios_2021", stringsAsFactors = FALSE,
                      encoding = "UTF-8")
df_estados <- readOGR("BR", "BR_UF_2021", stringsAsFactors=FALSE,
                   encoding="UTF-8")

################################################################
# Exploração, transformação e preparação dos dados
################################################################
# Exibir dimensoes
dim(df_energia) # 5570    6
dim(df_pib) # 5574    2

# Exibir tipo das colunas e respectivas amostras
glimpse(df_energia) # representação bastante irregular dos dados
glimpse(df_pib) # representação bastante irregular dos dados

# Exibir primeiras linhas dos dados
head(df_energia) # formato não convencional de linhas x colunas; necessita tratamento
head(df_pib) # formato não convencional de linhas x colunas; necessita tratamento

# Removendo linhas inuteis
df_energia <- df_energia[5:5569,]
df_pib <- df_pib[4:5573,]

# Renomeando colunas
colnames(df_energia) <- c("municipio","total", "tem_medidor", "tem_medidor_exclusivo",
                       "tem_medidor_comum", "nao_tem_medidor")
colnames(df_pib) <- c("municipio", "pib")

# Criar a coluna sigla_estado
df_energia$estado_sigla <- toupper(str_sub(df_energia$municipio, -3, -2))
df_pib$estado_sigla <- toupper(str_sub(df_pib$municipio, -3, -2))

# Remover o estado do campo municipio
df_energia$municipio <- str_sub(df_energia$municipio, 0, -6)
df_pib$municipio <- str_sub(df_pib$municipio, 0, -6)


# Transformando os dados em tipo numérico p/ permitir computações
df_pib$pib = as.numeric(df_pib$pib)
df_pib$pib[is.na(df_pib$pib)] = 0

df_energia$tem_medidor = as.numeric(df_energia$tem_medidor)
df_energia$tem_medidor[is.na(df_energia$tem_medidor)] = 0

df_energia$total = as.numeric(df_energia$total)
df_energia$total[is.na(df_energia$total)] = 0

df_energia$tem_medidor_exclusivo = as.numeric(df_energia$tem_medidor_exclusivo)
df_energia$tem_medidor_exclusivo[is.na(df_energia$tem_medidor_exclusivo)] = 0

df_energia$tem_medidor_comum = as.numeric(df_energia$tem_medidor_comum)
df_energia$tem_medidor_comum[is.na(df_energia$tem_medidor_comum)] = 0

df_energia$nao_tem_medidor = as.numeric(df_energia$nao_tem_medidor)
df_energia$nao_tem_medidor[is.na(df_energia$nao_tem_medidor)] = 0



################################################################
# Energia e PIB por ESTADO
################################################################

######### PIB ############
# Agrupando por estado_sigla e somando o pib
df_pib_estado <- df_pib %>% group_by(estado_sigla) %>% summarize(
  pib_estado = sum(pib,na.rm = T))

# Agrupando por estado_sigla e somando os medidores
df_energia_estado <- df_energia %>% group_by(estado_sigla) %>% summarize(
  energia_estado = sum(tem_medidor, na.rm = T))

# Unindo dados do Estado
df_dados_estados <- merge(df_energia_estado, df_pib_estado, by.x = "estado_sigla", by.y = "estado_sigla")

# Unindo os dados dos estdos aos dados geograficos
geo_dados_estados <- merge(df_estados, df_dados_estados, by.x = "SIGLA", by.y = "estado_sigla")

######### EXPLORAÇÃO ESTATÍSTICA ############

summary(df_energia_estado)
summary(df_pib_estado)

boxplot(df_energia_estado$energia_estado)
boxplot(df_pib_estado$pib_estado)


ggplot(data=df_energia_estado,aes(x=reorder(estado_sigla,energia_estado),y=(energia_estado))) + 
  geom_bar(stat ='identity',aes(fill=(energia_estado)))+
  coord_flip() + 
  theme_grey() + 
  scale_fill_gradient(name="Nível de Energia")+
  labs(title = 'Ranking dos Estados por Energia',
       y='Energia',x='Estados')+ 
  geom_hline(yintercept = mean((df_energia_estado$energia_estado)),size = 1, color = 'blue')

options(scipen = 999)
ggplot(data=df_pib_estado,aes(x=reorder(estado_sigla,pib_estado),y=(pib_estado))) + 
  geom_bar(stat ='identity',aes(fill=(pib_estado)))+
  coord_flip() + 
  theme_grey() + 
  scale_fill_gradient(name="Valor do PIB")+
  labs(title = 'Ranking dos Estados por PIB',
       y='PIB',x='Estados')+ 
  geom_hline(yintercept = mean((df_pib_estado$pib_estado)),size = 1, color = 'blue')


ggcorr(df_dados_estados, method = c("everything", "pearson")) 

######### MAPA PIB ############
proj4string(geo_dados_estados) <- CRS("+proj=longlat +datum=WGS84 +no_defs")
pal <- colorBin("Blues", domain = NULL, n=5)
state_popup <- paste0("<strong>Estado:</strong>", geo_dados_estados$SIGLA, 
                      "<br><strong>PIB: </strong>", geo_dados_estados$pib_estado)
leaflet(data = geo_dados_estados)%>%
  addProviderTiles("CartoDB.Positron")%>%
  addPolygons(fillColor = ~pal(geo_dados_estados$pib_estado), fillOpacity = 0.8,
              color = "#BDBDC3", weight = 1, popup = state_popup)%>%
  addLegend("bottomright", pal = pal, values = ~geo_dados_estados$pib_estado,
            title = "PIB por Estado", opacity = 1)

######### MAPA ENERGIA ############
proj4string(geo_dados_estados) <- CRS("+proj=longlat +datum=WGS84 +no_defs")
pal <- colorBin("Blues", domain = NULL, n=5)
state_popup <- paste0("<strong>Estado:</strong>", geo_dados_estados$SIGLA, 
                      "<br><strong>ENERGIA: </strong>", geo_dados_estados$energia_estado)
leaflet(data = geo_dados_estados)%>%
  addProviderTiles("CartoDB.Positron")%>%
  addPolygons(fillColor = ~pal(geo_dados_estados$energia_estado), fillOpacity = 0.8,
              color = "#BDBDC3", weight = 1, popup = state_popup)%>%
  addLegend("bottomright", pal = pal, values = ~geo_dados_estados$energia_estado,
            title = "ENERGIA por Estado", opacity = 1)


################################################################
# TOP 3 PIBs Estados
################################################################
top_3_estados <- head(df_dados_estados[order(df_dados_estados$pib_estado, 
                                             decreasing = TRUE), ], n=3)
top_3_estados # SP, RJ e MG

# Dados geograficos dos top 3 estados
municipios_sp <- readOGR("TOP3", "SP_Municipios_2021", stringsAsFactors = FALSE,
                         encoding = "UTF-8")
municipios_rj <- readOGR("TOP3", "RJ_Municipios_2021", stringsAsFactors = FALSE,
                         encoding = "UTF-8")
municipios_mg <- readOGR("TOP3", "MG_Municipios_2021", stringsAsFactors = FALSE,
                         encoding = "UTF-8")

# Jogando os dataframes p/ o spark
df_pib_spark <- sdf_copy_to(sc, df_pib, overwrite = T)
df_energia_spark <- sdf_copy_to(sc, df_energia, overwrite = T)
top_3_estados_spark <- sdf_copy_to(sc, top_3_estados, overwrite = T)

# Unindo os dataframes de pib e energia por cidade e estado
df_dados_municipios <- dbGetQuery(sc, "SELECT p.municipio, p.estado_sigla, p.pib, e.tem_medidor as energia
           from df_pib p inner join df_energia e on p.municipio = e.municipio
           AND p.estado_sigla = e.estado_sigla")

# jogando o dataframe no spark
df_dados_municipios_spark <- sdf_copy_to(sc, df_dados_municipios, overwrite = T)

# unindo ao top_3 para filtrar a massa de dados somente com os municipios dos top 3 estados
# SP
df_dados_municipios_sp <- dbGetQuery(sc, "SELECT
  m.municipio AS NM_MUN, m.estado_sigla AS SIGLA, m.pib, m.energia
 FROM df_dados_municipios m inner join top_3_estados t 
           ON t.estado_sigla = m.estado_sigla WHERE m.estado_sigla = 'SP'")
#RJ
df_dados_municipios_rj <- dbGetQuery(sc, "SELECT
  m.municipio AS NM_MUN, m.estado_sigla AS SIGLA, m.pib, m.energia
 FROM df_dados_municipios m inner join top_3_estados t 
           ON t.estado_sigla = m.estado_sigla WHERE m.estado_sigla = 'RJ'")
#MG
df_dados_municipios_mg <- dbGetQuery(sc, "SELECT
  m.municipio AS NM_MUN, m.estado_sigla AS SIGLA, m.pib, m.energia
 FROM df_dados_municipios m inner join top_3_estados t 
           ON t.estado_sigla = m.estado_sigla WHERE m.estado_sigla = 'MG'")

# Unindo os dataframes aos dados geograficos
geo_dados_municipios_sp <- merge(municipios_sp, df_dados_municipios_sp, by=c("NM_MUN","SIGLA"))
geo_dados_municipios_rj <- merge(municipios_rj, df_dados_municipios_rj, by=c("NM_MUN","SIGLA"))
geo_dados_municipios_mg <- merge(municipios_mg, df_dados_municipios_mg, by=c("NM_MUN","SIGLA"))

######### MAPA MUNICIPIOS ############
#SP
proj4string(geo_dados_municipios_sp) <- CRS("+proj=longlat +datum=WGS84 +no_defs")
pal <- colorBin("Blues", domain = NULL, n=5)
state_popup <- paste0("<strong>Cidade:</strong>", geo_dados_municipios_sp$NM_MUN, 
                      "<br><strong>PIB: </strong>", geo_dados_municipios_sp$pib,
                      "<br><strong>ENERGIA: </strong>", geo_dados_municipios_sp$energia)
leaflet(data = geo_dados_municipios_sp)%>%
  addProviderTiles("CartoDB.Positron")%>%
  addPolygons(fillColor = ~pal(geo_dados_municipios_sp$pib), fillOpacity = 0.8,
              color = "#BDBDC3", weight = 1, popup = state_popup)%>%
  addLegend("bottomright", pal = pal, values = ~geo_dados_municipios_sp$pib,
            title = "São Paulo", opacity = 1)

#RJ
proj4string(geo_dados_municipios_rj) <- CRS("+proj=longlat +datum=WGS84 +no_defs")
pal <- colorBin("Blues", domain = NULL, n=5)
state_popup <- paste0("<strong>Cidade:</strong>", geo_dados_municipios_rj$NM_MUN, 
                      "<br><strong>PIB: </strong>", geo_dados_municipios_rj$pib,
                      "<br><strong>ENERGIA: </strong>", geo_dados_municipios_rj$energia)
leaflet(data = geo_dados_municipios_rj)%>%
  addProviderTiles("CartoDB.Positron")%>%
  addPolygons(fillColor = ~pal(geo_dados_municipios_rj$pib), fillOpacity = 0.8,
              color = "#BDBDC3", weight = 1, popup = state_popup)%>%
  addLegend("bottomright", pal = pal, values = ~geo_dados_municipios_rj$pib,
            title = "Rio de Janeiro", opacity = 1)
#MG
proj4string(geo_dados_municipios_mg) <- CRS("+proj=longlat +datum=WGS84 +no_defs")
pal <- colorBin("Blues", domain = NULL, n=5)
state_popup <- paste0("<strong>Cidade:</strong>", geo_dados_municipios_mg$NM_MUN, 
                      "<br><strong>PIB: </strong>", geo_dados_municipios_mg$pib,
                      "<br><strong>ENERGIA: </strong>", geo_dados_municipios_mg$energia)
leaflet(data = geo_dados_municipios_mg)%>%
  addProviderTiles("CartoDB.Positron")%>%
  addPolygons(fillColor = ~pal(geo_dados_municipios_mg$pib), fillOpacity = 0.8,
              color = "#BDBDC3", weight = 1, popup = state_popup)%>%
  addLegend("bottomright", pal = pal, values = ~geo_dados_municipios_mg$pib,
            title = "Minas Gerais", opacity = 1)
