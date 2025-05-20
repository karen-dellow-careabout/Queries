WITH LeadRequestMod AS (
  SELECT 
    Object_lr,
    LeadReqId_lr,
    CreatedDateTimeLeadReq_lr,
    CAST(CreatedDateTimeLeadReq_lr AS date) AS CreatedDateOnly,
CASE
  WHEN 
    DATENAME(WEEKDAY, SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time') = 'Monday' 
    AND CAST(CreatedDateTimeLeadReq_lr AS date) IN (
      CAST(DATEADD(DAY, -2, SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time') AS date),
      CAST(DATEADD(DAY, -1, SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time') AS date)
    )
  THEN 1

  WHEN 
    CAST(CreatedDateTimeLeadReq_lr AS date) = CAST(DATEADD(DAY, -1, SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time') AS date)
  THEN 1

  ELSE 0
END AS YesterdaysContacts,
CASE
  WHEN 
    CAST(CreatedDateTimeLeadReq_lr AS date) >= CAST(
      DATEADD(DAY, 1 - DAY(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time'), 
              SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time')
      AS date
    )
    AND 
    CAST(CreatedDateTimeLeadReq_lr AS date) < CAST(
      DATEADD(MONTH, 1, 
        DATEADD(DAY, 1 - DAY(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time'), 
                SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time'))
      AS date
    )
  THEN 1
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
    StateForTimeZone_lr,
    ProductInterest_lr,
    CampaignName_lr,
    HcpLevel_lr,
    HcAssessmentStage_lr,
    Status_lr,
    ClickSource_lr,
    Coverage_lr,
    CONVERT(VARCHAR(8), CreatedDateTimeLeadReq_lr, 108) as TimeOfCreation,
    CASE
        WHEN CONVERT(VARCHAR(8), CreatedDateTimeLeadReq_lr, 108) BETWEEN '09:00:00' AND '19:00:00'
            THEN 1
        ELSE 0
    END AS WithinTimeWindow,
    ROW_NUMBER() OVER (PARTITION BY CompanyName_lr,  CAST(CreatedDateTimeLeadReq_lr AS date) ORDER BY CreatedDateTimeLeadReq_lr desc) AS seq
  FROM [careabout-db].dbo.ViewLeadRequestMod
  WHERE 
  --CreatedDateTimeLeadReq_lr BETWEEN '2025-05-01 00:00:00.000' AND '2025-05-31 23:59:59.000'
	HcAssessmentStage_lr IN ('Newly Funded','Switching','Newly Funded - Scheduled')
    AND Coverage_lr = 'Coverage'
     AND Status_lr NOT IN ('Duplicate', 'Invalid details')
     AND (CampaignName_lr IN ('Homecare All','Hunting Bunnies','Phone','Home Care back-up','Hot Funded','Pre Registration New Funnel','Approved New Funnel') 
     or (CampaignName_lr IS NULL and LeadType_lr = 'Phone')) 
     AND LastName_lr != 'test'
	AND FirstName_lr  != 'test'
	AND ((Email_lr NOT LIKE '%\@careabout%' AND Email_lr NOT LIKE '%\@test%') OR Email_lr IS NULL)
	
	
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
    StateForTimeZone_lr,
    ProductInterest_lr,
    CampaignName_lr,
    HcpLevel_lr,
    HcAssessmentStage_lr,
    Status_lr,
    ClickSource_lr,
    Coverage_lr,
    TimeOfCreation,
   WithinTimeWindow,
   if 
FROM LeadRequestMod
where seq = 1
