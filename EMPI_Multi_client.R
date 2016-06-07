library(sqldf)
library(plyr)
library(dplyr)
library(PGRdup)
library(data.table)
library(gdata)
library(xlsx)

#wd <-getwd()
#setwd('..')
source('~/R/Projects/SharedDataScripts/CommonFunctions.R')
#setwd(wd)


#set stuff up

Env <- 'Prod'
ClientName <- 'EHI'

Parms<-fnGetPopulationSettings(ClientName,Env)



ODSDB <-as.character(Parms[1,"ODSDBName"])
AppDSDB <-as.character(Parms[1,"AppDSDBName"])
PopulationType <- as.character(Parms[1,"PopulationType"])
ODSServerAddress <- as.character(Parms[1,"ODSServerAddress"])
AppDSServerAddress <- as.character(Parms[1,"AppDSServerAddress"])
IPAASServerAddress <- as.character(Parms[1,"IPAASServerAddress"])


Databases <- fnQueryDatabaseGeneric(ODSServerAddress,'master',"SELECT name AS DBName FROM sys.databases WHERE name LIKE '%ODS'")


sQuery<-"SET NOCOUNT ON

DECLARE @SQL varchar(1000)
DECLARE @DBNAME varchar(100)

SET @DBNAME = REPLACE(DB_NAME(),'ODS','TRANS')

CREATE TABLE #TMP_MBR
(
  CLIENT_ID varchar(10),
  SYS_MBR_SK INT,
  MBR_LAST_NM VARCHAR(100),
  MBR_FIRST_NM VARCHAR(100),
  MBR_DOB DATE,
  MBR_GENDER_CD CHAR(1),
  MBR_SSN VARCHAR(20)
)

SET @SQL ='INSERT INTO #TMP_MBR SELECT DISTINCT CLIENT_ID AS ENTITY_ID, A.SYS_MBR_SK, MBR_LAST_NM, MBR_FIRST_NM, MBR_DOB, MBR_GENDER_CD, MBR_SSN
FROM '+@DBNAME+'.dbo.STG_MBR_ARCHIVE A'

EXEC(@SQL)

SELECT DISTINCT A.CLIENT_ID AS ENTITY_ID,A.SYS_MBR_SK, A.MBR_LAST_NM, A.MBR_FIRST_NM, A.MBR_DOB, A.MBR_GENDER_CD, C.SYS_SUBSC_SK, A.MBR_SSN,B.EMPI,
RTRIM(D.EMAIL_ADDR_TXT) AS EMAIL, 
D.PHONE_NBR, LEFT(D.ZIP_CD,5) AS ZIP_CD,
MBR_HICN
FROM #TMP_MBR A
JOIN MBR B ON A.SYS_MBR_SK = B.SYS_MBR_SK
JOIN SUBSC_MBR C ON B.SYS_MBR_SK = C.SYS_MBR_SK
LEFT JOIN MBR_ADDR_ALL D ON B.SYS_MBR_SK = D.SYS_MBR_SK
LEFT JOIN MBR_HICN E ON B.SYS_MBR_SK = E.SYS_MBR_SK
"
ServerAddress<-ODSServerAddress
ODS <-fnForEachDB(sQuery)
#ODS <-fnQueryDatabaseGeneric(ODSServerAddress,ODSDB,sQuery)

#saveRDS(ODS, "ODS.rds")

sQuery <-"
SELECT A.PATIENT_PROB_LINK_ID,A.PATIENT_PROB_MATCH_ID,
P1.ENTITY_ID AS P1_ENTITY_ID,
P1.PATIENT_ID AS P1_PATIENT_ID,
P1.PATIENT_LAST_NAME AS P1_PATIENT_LAST_NAME,
P1.PATIENT_FIRST_NAME AS P1_PATIENT_FIRST_NAME,
LEFT(P1.PATIENT_ZIP,5) AS P1_PATIENT_ZIP,
P1.PATIENT_H_PH AS P1_PATIENT_H_PH,
P1.PATIENT_EMAIL AS P1_PATIENT_EMAIL,
P1.LAST_NAME_METAPHONE AS P1_LAST_NAME_METAPHONE,
P1.FIRST_NAME_METAPHONE AS P1_FIRST_NAME_METAPHONE,
P1.PATIENT_DOB AS P1_PATIENT_DOB,
P1.PATIENT_SSN AS P1_PATIENT_SSN,
P1.ENTITY_PATIENT_ID AS P1_ENTITY_PATIENT_ID,
P1.PATIENT_GENDER AS P1_PATIENT_GENDER,
P2.ENTITY_ID AS P2_ENTITY_ID,
P2.PATIENT_ID AS P2_PATIENT_ID,
P2.PATIENT_LAST_NAME AS P2_PATIENT_LAST_NAME,
P2.PATIENT_FIRST_NAME AS P2_PATIENT_FIRST_NAME,
LEFT(P2.PATIENT_ZIP,5) AS P2_PATIENT_ZIP,
P2.PATIENT_H_PH AS P2_PATIENT_H_PH,
P2.PATIENT_EMAIL AS P2_PATIENT_EMAIL,
P2.LAST_NAME_METAPHONE AS P2_LAST_NAME_METAPHONE,
P2.FIRST_NAME_METAPHONE AS P2_FIRST_NAME_METAPHONE,
P2.PATIENT_DOB AS P2_PATIENT_DOB,
P2.PATIENT_SSN AS P2_PATIENT_SSN,
P2.ENTITY_PATIENT_ID AS P2_ENTITY_PATIENT_ID,
P2.PATIENT_GENDER AS P2_PATIENT_GENDER
FROM IPAAS.PATIENT_PROB_LINKS A
JOIN IPAAS.PATIENT_PROB_MATCH B ON A.PATIENT_PROB_MATCH_ID = B.PATIENT_PROB_MATCH_ID
JOIN IPAAS.PATIENT_MASTER P1 ON A.LINKED_PATIENT_ID = P1.PATIENT_ID
JOIN IPAAS.PATIENT_MASTER P2 ON B.PATIENT_ID = P2.PATIENT_ID
"
IPAAS <-fnQueryDatabaseGeneric(IPAASServerAddress,'IPAAS',sQuery)

ODS_FIRST_NM <- select(ODS,MBR_FIRST_NM) %>%
  distinct %>%
  rowwise()%>%
  mutate (MBR_FIRST_NM_METAPHONE = DoubleMetaphone(MBR_FIRST_NM)$primary)

ODS_LAST_NM <- select(ODS,MBR_LAST_NM) %>%
  distinct %>%
  rowwise()%>%
  mutate (MBR_LAST_NM_METAPHONE = DoubleMetaphone(MBR_LAST_NM)$primary)

ODS2 <-ODS %>%
  inner_join(ODS_FIRST_NM)%>%
  inner_join(ODS_LAST_NM)
# rowwise()%>%
# mutate(MBR_LAST_NM_METAPHONE = DoubleMetaphone(MBR_LAST_NM)$primary,
#        MBR_FIRST_NM_METAPHONE = DoubleMetaphone(MBR_FIRST_NM)$primary
# )

#saveRDS(ODS2, "ODS2.rds")
# ODS2 <-  readRDS("ODS2.rds")

rm(ODS)


LFT <- IPAAS%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P1_ENTITY_ID)%>%
  inner_join(ODS2,by=c("P1_ENTITY_PATIENT_ID"="SYS_MBR_SK","P1_ENTITY_ID"="ENTITY_ID"))

RGHT <- IPAAS%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P2_ENTITY_ID)%>%
  inner_join(ODS2,by=c("P2_ENTITY_PATIENT_ID"="SYS_MBR_SK","P2_ENTITY_ID"="ENTITY_ID"))


Matches <-
  rbind(
    inner_join(LFT,filter(RGHT,is.na(MBR_SSN)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_SSN"="MBR_SSN")) %>%
      distinct%>% filter(trim(MBR_SSN) != '') %>%
      mutate(Reason='ODSSSN',
             Attr=MBR_SSN)%>%
      select(1:2,Reason,Attr)
    ,
    inner_join(LFT,filter(RGHT,is.na(PHONE_NBR)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_DOB"="MBR_DOB","MBR_LAST_NM_METAPHONE"="MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE"="MBR_FIRST_NM_METAPHONE","PHONE_NBR"="PHONE_NBR")) %>%
      distinct%>% filter(trim(PHONE_NBR) != '') %>%
      mutate(Reason='ODSPhone',
             Attr=PHONE_NBR)%>%
      select(1:2,Reason,Attr)
    ,
    inner_join(LFT,filter(RGHT,is.na(EMAIL)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_DOB","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE","EMAIL")) %>%
      distinct%>% filter(trim(EMAIL) != '') %>%
      mutate(Reason='ODSEmail',
             Attr=EMAIL)%>%
      select(1:2,Reason,Attr)
    ,
    inner_join(LFT,filter(RGHT,is.na(SYS_SUBSC_SK)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_DOB","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE","SYS_SUBSC_SK","P1_ENTITY_ID"="P2_ENTITY_ID")) %>%
      distinct%>% 
      mutate(Reason='ODSSubsc',
             Attr=SYS_SUBSC_SK)%>%
      select(1:2,Reason,Attr)
    ,
    
    inner_join(LFT,filter(RGHT,is.na(MBR_HICN)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE","MBR_HICN")) %>%
      distinct%>%
      mutate(Reason='ODSHICN',
             Attr=MBR_HICN)%>%
      select(1:2,Reason,Attr)
    ,
    inner_join(LFT,RGHT,by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_GENDER_CD","MBR_DOB","ZIP_CD","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE")) %>%
      distinct%>%
      mutate(Reason='ODSZip',
             Attr=ZIP_CD)%>%
      select(1:2,Reason,Attr)
    
  )

colnames(Matches)<-c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","Reason","Attr")

MatchesRpt <- Matches%>%
  distinct%>%
  group_by(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,Reason)%>%
  summarise(attr=max(Attr))%>%
  spread(Reason,attr)%>%
  inner_join(select(IPAAS,1:3,P2_ENTITY_ID,P1_ENTITY_PATIENT_ID,P2_ENTITY_PATIENT_ID))

#######go through it again

ODS3 <-inner_join(ungroup(MatchesRpt),ODS2,by=c("P2_ENTITY_ID"="ENTITY_ID","P2_ENTITY_PATIENT_ID"="SYS_MBR_SK"))%>%
  select(P1_ENTITY_ID,P1_ENTITY_PATIENT_ID,MBR_LAST_NM,MBR_FIRST_NM,MBR_DOB,MBR_GENDER_CD,SYS_SUBSC_SK,MBR_SSN,EMPI,EMAIL,PHONE_NBR,ZIP_CD,MBR_HICN,MBR_FIRST_NM_METAPHONE,MBR_LAST_NM_METAPHONE)

colnames(ODS3)[1] <- "ENTITY_ID"
colnames(ODS3)[2] <- "SYS_MBR_SK"

ODS3 <-rbind(ODS3,ODS4)%>%
  distinct


LFT2 <- IPAAS%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P1_ENTITY_ID)%>%
  inner_join(ODS3,by=c("P1_ENTITY_PATIENT_ID"="SYS_MBR_SK","P1_ENTITY_ID"="ENTITY_ID"))

RGHT2 <- IPAAS%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P2_ENTITY_ID)%>%
  inner_join(ODS3,by=c("P2_ENTITY_PATIENT_ID"="SYS_MBR_SK","P2_ENTITY_ID"="ENTITY_ID"))



Matches2 <-
  rbind(
    inner_join(LFT2,filter(RGHT2,is.na(MBR_SSN)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_SSN")) %>%
      distinct%>%
      mutate(Reason='ODSSSN',
             Attr=MBR_SSN)%>%
      select(1:2,Reason,Attr)
    ,
    inner_join(LFT2,filter(RGHT,is.na(PHONE_NBR)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_DOB","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE","PHONE_NBR")) %>%
      distinct%>%
      mutate(Reason='ODSPhone',
             Attr=PHONE_NBR)%>%
      select(1:2,Reason,Attr)
    ,
    inner_join(LFT2,filter(RGHT2,is.na(EMAIL)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_DOB","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE","EMAIL")) %>%
      distinct%>%
      mutate(Reason='ODSEmail',
             Attr=EMAIL)%>%
      select(1:2,Reason,Attr)
    ,
    inner_join(LFT2,filter(RGHT2,is.na(SYS_SUBSC_SK)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_DOB","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE","SYS_SUBSC_SK","P1_ENTITY_ID"="P2_ENTITY_ID")) %>%
      distinct%>%
      mutate(Reason='ODSSubsc',
             Attr=SYS_SUBSC_SK)%>%
      select(1:2,Reason,Attr)
    ,
    
    inner_join(LFT2,filter(RGHT2,is.na(MBR_HICN)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE","MBR_HICN")) %>%
      distinct%>%
      mutate(Reason='ODSHICN',
             Attr=MBR_HICN)%>%
      select(1:2,Reason,Attr)
    ,
    inner_join(LFT2,RGHT2,by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_GENDER_CD","MBR_DOB","ZIP_CD","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE")) %>%
      distinct%>%
      mutate(Reason='ODSZip',
             Attr=ZIP_CD)%>%
      select(1:2,Reason,Attr)
    
  )

colnames(Matches2)<-c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","Reason","Attr")

Matches2Rpt <- anti_join(Matches2,Matches,by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID"))%>%
  distinct%>%
  group_by(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,Reason)%>%
  summarise(attr=max(Attr))%>%
  spread(Reason,attr)%>%
  inner_join(select(IPAAS,1:3,P2_ENTITY_ID,P1_ENTITY_PATIENT_ID,P2_ENTITY_PATIENT_ID))

########WHO IS LEFT

Remainder <- select(IPAAS,PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID)%>%
  anti_join(select(MatchesRpt,PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID))%>%
  anti_join(select(Matches2Rpt,PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID))%>%
  inner_join(IPAAS)
# saveRDS(Remainder, "Remainder_1.rds")
#  Remainder1 <-  readRDS("Remainder_1.rds")
########CLAIMS

sQuery <-"
SELECT A.CLIENT_ID,A.SYS_MBR_SK,LEFT(PROV_NPI,10) AS NPI, COUNT(*) AS CLMS
FROM CLAIM A
JOIN PROV B ON A.PROV_SK = B.PROV_SK JOIN CLAIM_DTL C ON C.CLAIM_SK = A.CLAIM_SK
WHERE CLAIM_BEG_SVC_DT > DATEADD(YEAR,-1,GETDATE()) AND C.ENC_ID IS NOT NULL
GROUP BY A.CLIENT_ID,A.SYS_MBR_SK,LEFT(PROV_NPI,10)"

ODS_MBR_CLMS <- fnForEachDB(sQuery)

# saveRDS(ODS_MBR_CLMS,"ODS_MBR_CLMS.rds")
#ODS_MBR_CLMS <-  readRDS("ODS_MBR_CLMS.rds")

#ODS_MBR_CLMS2 <-  ODS_MBR_CLMS
#ODS_MBR_CLMS <-  ODS_MBR_CLMS2

ODS_MBR_CLMS  <- ODS_MBR_CLMS %>% inner_join (ODS2, by = c("SYS_MBR_SK", "CLIENT_ID" = "ENTITY_ID"))

NoClms <-Remainder%>%
  anti_join(ODS_MBR_CLMS,by=c("P1_ENTITY_ID"="CLIENT_ID","P1_ENTITY_PATIENT_ID"="SYS_MBR_SK"))%>%
  anti_join(ODS_MBR_CLMS,by=c("P2_ENTITY_ID"="CLIENT_ID","P2_ENTITY_PATIENT_ID"="SYS_MBR_SK"))

Remainder <-anti_join(Remainder,NoClms) 



LFT <- Remainder%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P1_ENTITY_ID)%>%
  inner_join(ODS_MBR_CLMS,by=c("P1_ENTITY_PATIENT_ID"="SYS_MBR_SK","P1_ENTITY_ID"="CLIENT_ID"))

RGHT <- Remainder%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P2_ENTITY_ID)%>%
  inner_join(ODS_MBR_CLMS,by=c("P2_ENTITY_PATIENT_ID"="SYS_MBR_SK","P2_ENTITY_ID"="CLIENT_ID"))



Matches <-
  # rbind(
  
  
  inner_join(filter(LFT,CLMS>=2),filter(RGHT,is.na(NPI)==FALSE&CLMS>=2),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","NPI", "MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE", "MBR_DOB")) %>%
  distinct%>%
  mutate(Reason='ClaimNPI',
         Attr=NPI)%>%
  select(1:2,Reason,Attr)
#Restrict based on encounter

# )

colnames(Matches)<-c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","Reason","Attr")

MatchesClaimsRpt <- Matches%>%
  distinct%>%
  group_by(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,Reason)%>%
  summarise(attr=max(Attr))%>%
  spread(Reason,attr)%>%
  inner_join(select(IPAAS,1:3,P2_ENTITY_ID,P1_ENTITY_PATIENT_ID,P2_ENTITY_PATIENT_ID))


Remainder <-anti_join(Remainder,MatchesClaimsRpt)

######PCP
sQuery <-"SELECT A.CLIENT_ID,SYS_MBR_SK,LEFT(PROV_NPI,10) AS NPI
FROM MBR_ASSOC_PROV_ALL A
JOIN PROV B ON A.MBR_ASSOC_PROV_SK = B.PROV_SK AND PROV_ID NOT LIKE 'DMY%'
GROUP BY A.CLIENT_ID,SYS_MBR_SK,LEFT(PROV_NPI,10)"

ODS_ATTR <- fnForEachDB(sQuery)

#saveRDS(ODS_ATTR, "ODS_ATTR.rds")
#ODS_ATTR <-  readRDS("ODS_ATTR.rds")

ODS_ATTR <-  ODS_ATTR %>% inner_join (ODS2, by = c("SYS_MBR_SK", "CLIENT_ID" = "ENTITY_ID"))



LFT <- Remainder%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P1_ENTITY_ID)%>%
  inner_join(ODS_MBR_CLMS,by=c("P1_ENTITY_PATIENT_ID"="SYS_MBR_SK","P1_ENTITY_ID"="CLIENT_ID"))

RGHT <- Remainder%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P2_ENTITY_ID)%>%
  inner_join(ODS_ATTR,by=c("P2_ENTITY_PATIENT_ID"="SYS_MBR_SK","P2_ENTITY_ID"="CLIENT_ID"))



Matches <-
  # rbind(
  
  
  inner_join(LFT,filter(RGHT,is.na(NPI)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","NPI", "MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE", "MBR_DOB")) %>%
  distinct%>%
  mutate(Reason='PCPNPI',
         Attr=NPI)%>%
  select(1:2,Reason,Attr)


# )

colnames(Matches)<-c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","Reason","Attr")

MatchesPCPRpt <- Matches%>%
  distinct%>%
  group_by(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,Reason)%>%
  summarise(attr=max(Attr))%>%
  spread(Reason,attr)%>%
  inner_join(select(IPAAS,1:3,P2_ENTITY_ID,P1_ENTITY_PATIENT_ID,P2_ENTITY_PATIENT_ID))



######OLD CLIENT FLB RECORDS --  REMOVE --  CONDITION --   WHERE P1_ENTITY_ID = 1 and P2_ENTITY_ID ==1

Remainder_BCBSF <-  Remainder
Remainder_BCBSF <-  filter(Remainder_BCBSF, P1_ENTITY_ID ==1 | P2_ENTITY_ID == 1)
Remainder <-  anti_join(Remainder, Remainder_BCBSF)

########RECORDS WHEN LFT ENTITY ID <> RIGHT ENTITY ID FOR ACTIVE MEDICARE MEMBERS###################

sQuery <-"select CLIENT_ID, SYS_MBR_SK, count(*) as 'cnt' from mbr_display_dtl where eff_dt <= getdate() and term_dt >= getdate() and REC_DEL_IND = 'N' AND lob_tp_id = 1
group by CLIENT_ID, SYS_MBR_SK"

ODS_ACTIVE_MBRS <- fnForEachDB(sQuery)
#saveRDS(ODS_ACTIVE_MBRS, "ODS_ACTIVE_MBRS.rds")
# ODS_ACTIVE_MBRS <-  readRDS("ODS_ACTIVE_MBRS.rds")

ODS_ACTIVE_MBRS <-  ODS_ACTIVE_MBRS %>% inner_join (ODS2, by = c("SYS_MBR_SK", "CLIENT_ID" = "ENTITY_ID"))

LFT <- Remainder%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P1_ENTITY_ID)%>%
  inner_join(ODS_ACTIVE_MBRS,by=c("P1_ENTITY_PATIENT_ID"="SYS_MBR_SK","P1_ENTITY_ID"="CLIENT_ID")) #%>% select(1:5)


RGHT <- Remainder%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P2_ENTITY_ID)%>%
  inner_join(ODS_ACTIVE_MBRS,by=c("P2_ENTITY_PATIENT_ID"="SYS_MBR_SK","P2_ENTITY_ID"="CLIENT_ID"))  #%>% select(1:5)

Matches <-
  # rbind(
  inner_join(LFT,RGHT,by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","MBR_LAST_NM_METAPHONE","MBR_FIRST_NM_METAPHONE" )) %>%
  distinct%>%
  mutate(Reason, Attr='Multiple Medicare Active Plan Records')%>%
  select(1:2,Reason,Attr)
# )

colnames(Matches)<-c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","Reason","Attr")

MatchesDupCoverage <- Matches%>%
  distinct%>%
  group_by(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,Reason)%>%
  summarise(attr=max(Attr))%>%
  spread(Reason,attr)%>%
  inner_join(select(IPAAS,1:3,P2_ENTITY_ID,P1_ENTITY_PATIENT_ID,P2_ENTITY_PATIENT_ID))

Remainder <-  anti_join(Remainder, MatchesDupCoverage) #IGNORE THESE RECORDS

#################################################################  EHI/EHI########################################################################

###########################################################################################################################################################

sQuery <-  "select CLIENT_ID, SYS_MBR_SK, EFF_DT, TERM_DT  from MBR_DISPLAY_DTL where Primary_COVRG_IND = 'Y' and getdate() between eff_Dt and term_dt and rec_del_ind = 'N'"

ODS_MBR_COVERAGE <- fnForEachDB(sQuery)

#  saveRDS(ODS_MBR_COVERAGE, "ODS_MBR_COVERAGE.rds")
# ODS_MBR_COVERAGE <-  readRDS("ODS_MBR_COVERAGE.rds")

LFT <- Remainder%>%
  filter(P1_ENTITY_ID ==P2_ENTITY_ID) %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P1_ENTITY_ID)%>%
  inner_join(ODS_MBR_COVERAGE,by=c("P1_ENTITY_PATIENT_ID"="SYS_MBR_SK","P1_ENTITY_ID"="CLIENT_ID")) %>% select(1:5, TERM_DT)


RGHT <- Remainder%>%
  filter(P1_ENTITY_ID ==P2_ENTITY_ID) %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P2_ENTITY_ID)%>%
  inner_join(ODS_MBR_COVERAGE,by=c("P2_ENTITY_PATIENT_ID"="SYS_MBR_SK","P2_ENTITY_ID"="CLIENT_ID"))  %>% select(1:5, TERM_DT)

Matches <-
  # rbind(
  inner_join(LFT,RGHT,by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID")) %>%
  distinct%>%
  mutate(Reason, Attr='DUP COVERAGE Same Client')%>%
  select(1:2,Reason,Attr)


# )

colnames(Matches)<-c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","Reason","Attr")


MatchesCoverage <- Matches%>% 
  distinct%>%
  group_by(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,Reason)%>%
  summarise(attr=max(Attr))%>%
  spread(Reason,attr)%>%
  inner_join(select(IPAAS,1:3,P2_ENTITY_ID,P1_ENTITY_PATIENT_ID,P2_ENTITY_PATIENT_ID))

Remainder <-  anti_join(Remainder, MatchesCoverage)  # IGNORE THESE RECORDS

rm(ODS2)
rm(ODS_ACTIVE_MBRS)
rm(ODS_MBR_COVERAGE)
rm(ODS_MBR_CLMS)
rm(ODS_MBR_SSN)
rm(ODS_ATTR)


Merge_records <-  rbind.fill(MatchesRpt, Matches2Rpt, MatchesClaimsRpt, MatchesPCPRpt)
Ignore_records <-  rbind.fill(MatchesCoverage, MatchesDupCoverage)

###################################WRITE THE DATA TO A SPREADSHEET###################################

write.csv(Merge_records, file = "C:/Users/sravichandar/Desktop/PROB_MATCH_MERGE.csv")
write.csv(Ignore_records, 'C:/Users/sravichandar/Desktop/PROB_MATCH_IGNORE.csv')
write.csv(Remainder_BCBSF, 'C:/Users/sravichandar/Desktop/PROB_MATCH_DELETE.csv')







#########################################################################################################






########PULL RECORDS WITH P1_ENTITY_ID = 2 AND P2_ENTITY_ID = 2
#SELECT * FROM MBR WHERE SYS_MBR_SK IN (106398,79580,74805,85184,45287) order by mbr_last_nm
#SELECT * FROM MBR WHERE SYS_MBR_SK IN (94830,57762, 104223, 95778, 104495) order by mbr_last_nm
#select * from mbr where sys_mbr_sk in (79580, 57762)
#select * from mbr_display_dtl where sys_mbr_sk in (79580) and rec_del_ind = 'N' --(106398,79580,74805,85184,45287)
#SELECT * FROM MBR_display_dtl WHERE SYS_MBR_SK IN (57762) and rec_del_ind = 'N'




###################IGNORE THE FOLLOWING RECORDS###################
#######  DATE OF DEATH (LEFT DATE OF DEATH <> RIGHT DATE OF DEATH)
#######  SSN (WHEN SSN IS NOT NULL LEFT SSN <> RIGHT SSN ==>  TOTALLY A WRONG PERSON TO MERGE)
#######  TWO ACTIVE COVERAGES FOR EXAMPLE ONE ACTIVE COVERAGE IN BKC AND ONE IN CHS. NOTPOSSIBLE FOR A MEDICARE MEMBER

#####select * from bkcods..mbr where mbr_id in ('000122373','000124414','000122519')
#####select * from bkcods..mbr_display_dtl where sys_mbr_sk in (1465,1524,2215) and rec_del_ind = 'N' and getdate() between eff_dt and term_dt
####select * from bkcods..MBR_COVRG where sys_mbr_sk in (1465,1524,2215) and rec_del_ind = 'N' and getdate() between eff_dt and term_dt
######select * from bkcods..MBR_LOB_ALL where sys_mbr_sk in (1465,1524,2215) and rec_del_ind = 'N' and getdate() between eff_dt and term_dt
#####select * from chsods..mbr where mbr_id in ('32763482','34580990','78096668201')
#####select * from chsods..mbr_display_Dtl where sys_mbr_sk in ('83422','464365','460383') and rec_del_ind = 'N' and getdate() between eff_dt and term_dt
######select * from chsods..mbr_COVRG where sys_mbr_sk in ('83422','464365','460383') and rec_del_ind = 'N' and getdate() between eff_dt and term_dt
######select * from chsods..mbr_LOB_ALL where sys_mbr_sk in ('83422','464365','460383') and rec_del_ind = 'N' and getdate() between eff_dt and term_dt


######RX_CLAIM##############################################################
sQuery <-"
select distinct  A.CLIENT_ID, A.SYS_MBR_SK, isnull(LEFT(C.PROV_NPI, 10),'') as NPI  
from RX_CLAIM A (nolock) join Prescriber B on  B.PRESCRIBER_ID = A.PRESCRIBER_ID 
join PROV C on C.PROV_SK = B.COMMON_PROV_SK AND PROV_ID NOT LIKE 'DMY%'
GROUP BY A.CLIENT_ID,SYS_MBR_SK,LEFT(PROV_NPI,10)"

ODS_RX <- fnForEachDB(sQuery)

Remainder <-anti_join(Remainder,MatchesPCPRpt)


LFT <- Remainder%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P1_ENTITY_ID)%>%
  inner_join(ODS_RX,by=c("P1_ENTITY_PATIENT_ID"="SYS_MBR_SK","P1_ENTITY_ID"="CLIENT_ID"))

RGHT <- Remainder%>%
  #filter(P1_ENTITY_ID =='3'&P2_ENTITY_ID =='3') %>%
  select(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,P1_ENTITY_PATIENT_ID, P2_ENTITY_PATIENT_ID,P2_ENTITY_ID)%>%
  inner_join(ODS_RX,by=c("P2_ENTITY_PATIENT_ID"="SYS_MBR_SK","P2_ENTITY_ID"="CLIENT_ID"))



Matches <-
  # rbind(
  
  
  inner_join(LFT,filter(RGHT,is.na(NPI)==FALSE),by=c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","NPI")) %>%
  distinct%>%
  mutate(Reason='RXNPI',
         Attr=NPI)%>%
  select(1:2,Reason,Attr)


# )

colnames(Matches)<-c("PATIENT_PROB_LINK_ID","PATIENT_PROB_MATCH_ID","Reason","Attr")

MatchesRxRpt <- Matches%>%
  distinct%>%
  group_by(PATIENT_PROB_LINK_ID,PATIENT_PROB_MATCH_ID,Reason)%>%
  summarise(attr=max(Attr))%>%
  spread(Reason,attr)%>%
  inner_join(select(IPAAS,1:3,P2_ENTITY_ID,P1_ENTITY_PATIENT_ID,P2_ENTITY_PATIENT_ID))


#######################CONVERSE VALIDATING ODS TO IPAAS EMPI#################################################


wrk_ODS2 <-select(ODS2,ENTITY_ID,SYS_MBR_SK,EMPI,MBR_LAST_NM_METAPHONE,MBR_FIRST_NM_METAPHONE,MBR_DOB,MBR_GENDER_CD,PHONE_NBR,ZIP_CD) %>%
  distinct

x <- wrk_ODS2 %>%
  inner_join(wrk_ODS2,by=c("MBR_DOB","MBR_GENDER_CD","MBR_FIRST_NM_METAPHONE","MBR_LAST_NM_METAPHONE","PHONE_NBR"))


x<-filter(x,`EMPI.x`!=`EMPI.y`&is.na(`PHONE_NBR`)==FALSE&is.na(`ZIP_CD.x`)==FALSE&is.na(`ZIP_CD.y`)==FALSE) 

x <-anti_join(x,IPAAS,by=c("ENTITY_ID.x"="P1_ENTITY_ID","SYS_MBR_SK.x"="P1_ENTITY_PATIENT_ID","ENTITY_ID.y"="P2_ENTITY_ID","SYS_MBR_SK.y"="P2_ENTITY_PATIENT_ID"))


wrk_ODS2 <-select(ODS2,ENTITY_ID,SYS_MBR_SK,EMPI,MBR_LAST_NM_METAPHONE,MBR_FIRST_NM_METAPHONE,MBR_DOB,MBR_GENDER_CD,MBR_SSN) %>%
  distinct

y <- wrk_ODS2 %>%
  inner_join(wrk_ODS2,by=c("MBR_DOB","MBR_GENDER_CD","MBR_FIRST_NM_METAPHONE","MBR_LAST_NM_METAPHONE","MBR_SSN"))


y<-filter(y,`EMPI.x`!=`EMPI.y`&is.na(`MBR_SSN`)==FALSE) 

y <-anti_join(y,IPAAS,by=c("ENTITY_ID.x"="P1_ENTITY_ID","SYS_MBR_SK.x"="P1_ENTITY_PATIENT_ID","ENTITY_ID.y"="P2_ENTITY_ID","SYS_MBR_SK.y"="P2_ENTITY_PATIENT_ID"))


###############################################################################################################

