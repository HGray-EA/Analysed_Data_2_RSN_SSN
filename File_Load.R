
# Categorisation of RSN/SSN monitoring sites using waterbody mapping typologies work by CSG agri 


library(tidyverse)
library(sf)
library(brickster)
library(leaflet)


WB_Cls <- read.csv("/dbfs/FileStore/WSX_HGray/ETL_Exports/MC_WB_Classifications.csv")

tmp_csv <- tempfile(fileext = ".csv")

db_volume_read(
  "/Volumes/prd_dash_lab/seda_restricted/ncea/networks/RSN/Data_Snapshots/rsn_sites.csv",
  tmp_csv,
  perform_request = TRUE
)

Sites.R <- read.csv(tmp_csv)

tmp <- db_volume_read(
  '/Volumes/prd_dash_bronze/defra_data_services_platform_unrestricted/wfd_river_waterbody_catchments_cycle_2/format_GEOPARQUET_wfd_river_waterbody_catchments_cycle_2/LATEST_wfd_river_waterbody_catchments_cycle_2/WFD_River_Water_Body_Catchments_Cycle_2.parquet',
  tempfile(),
  perform_request = TRUE
)

WBs <- arrow::read_parquet(tmp) #%>% st_as_sf()


WB <- WBs %>% filter(wb_cat == "River WB catchment")

WB_Cat <- inner_join(WB_Cls, 
                     WBs, 
                     by= c("EA_WB_ID"="wb_id")) %>% 
  st_as_sf() %>% 
  st_set_crs(27700) %>%
  st_transform(4326)
