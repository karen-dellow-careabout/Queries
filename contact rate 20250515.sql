
ALTER VIEW vw_ContactRateAnalysis AS


WITH LeadRequestMod AS (
  SELECT 
    Object_lr,
    LeadReqId_lr,
    CreatedDateTimeLeadReq_lr,
    --CAST(CreatedDateTimeLeadReq_lr AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS datetime) AS CreatedDateTime_AEST,
    CAST(CreatedDateTimeLeadReq_lr AS date) AS CreatedDateOnly,
      CASE
    WHEN CAST(CreatedDateTimeLeadReq_lr AS date) = CAST(DATEADD(DAY, -1, GETDATE()) AS date) THEN 1
     ELSE 0
END AS YesterdaysContacts,
     CASE
	    WHEN CAST(CreatedDateTimeLeadReq_lr AS date) <= CAST(EOMONTH(GETDATE()) AS date) THEN 1
    ELSE 0
END AS MTDContacts,
    FullName_lr,
    FirstName_lr,
    LastName_lr,
    CompanyName_lr,
    LeadType_lr,
    LeadSource_lr,
    CustomerPostalCode_lr,
    CustomerCitySuburb_lr,
    ProductInterest_lr,
    CampaignName_lr,
    HcpLevel_lr,
    HcAssessmentStage_lr,
    Status_lr,
    ClickSource_lr,
    Coverage_lr,
    CASE
        WHEN CONVERT(VARCHAR(8), CreatedDateTimeLeadReq_lr, 108) BETWEEN '09:00:00' AND '19:00:00'
            THEN 1
        ELSE 0
    END AS WithinTimeWindow,
    ROW_NUMBER() OVER (PARTITION BY CompanyName_lr  ORDER BY CreatedDateTimeLeadReq_lr desc) AS seq
  FROM [careabout-db].dbo.ViewLeadRequestMod
  WHERE CreatedDateTimeLeadReq_lr BETWEEN '2025-05-01 00:00:00.000' AND '2025-05-14 23:59:59.000'
	AND HcAssessmentStage_lr IN ('Newly Funded','Switching','Newly Funded - Scheduled')
    AND Coverage_lr = 'Coverage'
     AND Status_lr NOT IN ('Duplicate', 'Invalid details')
     AND (CampaignName_lr IN ('Homecare All','Hunting Bunnies','Phone','Home Care back-up','Hot Funded','Pre Registration New Funnel') or (CampaignName_lr IS NULL and LeadType_lr = 'Phone'))
   --and Object_lr ='Lead'
  
)

,LeadRequestMod_dedupe as (
select
    Object_lr,
    LeadReqId_lr,
    CreatedDateTimeLeadReq_lr,
    CreatedDateOnly,
    YesterdaysContacts,
    MTDContacts,
    FullName_lr,
    FirstName_lr,
    LastName_lr,
    CompanyName_lr,
    LeadType_lr,
    LeadSource_lr,
    CustomerPostalCode_lr,
    CustomerCitySuburb_lr,
    ProductInterest_lr,
    CampaignName_lr,
    HcpLevel_lr,
    HcAssessmentStage_lr,
    Status_lr,
    ClickSource_lr,
    Coverage_lr,
   WithinTimeWindow
FROM LeadRequestMod
WHERE seq = 1


)

,FirstContactLeadsReq AS (
  SELECT LeadId as LeadRequestId,
    CAST(CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS datetime) AS ContactDateTime_AEST,
    CAST(CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS date) AS ContactDate_AEST,
    NewValue,
   ROW_NUMBER() OVER (PARTITION BY LeadId  ORDER BY CAST(CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS date) asc) AS seq
  FROM [careabout-db].dbo.LeadHistory 
  WHERE 
  Field = 'Status' AND (NewValue in ('Contacted x1','Qualified') and NewValue is not Null)
  
  
UNION ALL

  SELECT ParentId as LeadRequestId,
    CAST(CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS datetime) AS ContactDateTime_AEST,
    CAST(CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS date) AS ContactDate_AEST,
    NewValue,
    ROW_NUMBER() OVER (PARTITION BY ParentId ORDER BY CAST(CreatedDate AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS date) asc) AS seq
  FROM [careabout-db].dbo.RequestHistory
  WHERE Field = 'Request_Status__c' AND NewValue IN ('Closed - Actioned') 
)

,FirstContactLeadsReq_dedupe as (
select
LeadRequestId,
ContactDateTime_AEST,
ContactDate_AEST,
NewValue
from 
FirstContactLeadsReq
where seq =1
)

--,FinalTable AS (
SELECT 
  lr.*,
  lh.ContactDate_AEST AS FirstContactDate,
  lh.ContactDateTime_AEST,
  lh.NewValue,
  CASE
    WHEN CONVERT(VARCHAR(8), lh.ContactDateTime_AEST, 108) BETWEEN '09:00:00' AND '19:00:00' and Status_lr in ('Qualified','Contacted x1','Closed - Actioned')
            THEN 1
        ELSE 0
    END AS ContactedWithinDay,
     DATEDIFF(MINUTE, CreatedDateTimeLeadReq_lr, ContactDateTime_AEST) AS MinutesDifference,
 CASE 
  WHEN CAST(lh.ContactDateTime_AEST AS date) >= CAST(DATEADD(DAY, 1 - DAY(GETDATE()), GETDATE()) AS date)
   AND CAST(lh.ContactDateTime_AEST AS date) < CAST(DATEADD(MONTH, 1, DATEADD(DAY, 1 - DAY(GETDATE()), GETDATE())) AS date)
   AND Status_lr in ('Qualified','Do not call', 'Does not qualify','Closed - Actioned') AND WithinTimeWindow = 1
  THEN 1
  ELSE 0
END AS ContactedWithinMonth,
  FORMAT(CreatedDateTimeLeadReq_lr, 'dddd') AS DayName
FROM LeadRequestMod_dedupe lr
LEFT JOIN FirstContactLeadsReq_dedupe lh
  ON lr.LeadReqId_lr = lh.LeadRequestId AND
  lr.CreatedDateOnly <= lh.ContactDate_AEST
  GROUP BY 
  lr.Object_lr,
  lr.LeadReqId_lr,
  CreatedDateTimeLeadReq_lr,
  lr.CreatedDateOnly,
  lr.YesterdaysContacts,
  lr.MTDContacts,
  lr.FullName_lr,
  lr.FirstName_lr,
  lr.LastName_lr,
  lr.CompanyName_lr,
  lr.LeadType_lr,
  lr.LeadSource_lr,
  lr.CustomerPostalCode_lr,
  lr.CustomerCitySuburb_lr,
  lr.ProductInterest_lr,
  lr.CampaignName_lr,
  lr.HcpLevel_lr,
  lr.HcAssessmentStage_lr,
  lr.Status_lr,
  lr.ClickSource_lr,
  lr.Coverage_lr,
  lh.ContactDate_AEST,
  lh.ContactDateTime_AEST,
  lh.NewValue,
  lr.WithinTimeWindow
  


