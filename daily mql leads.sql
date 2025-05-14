
--ALTER VIEW vw_ContactRateAnalysis AS

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
    Email_lr,
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
    WhoNeedsHelp_lr, 
    GaConnectorLastClickCampaign_lr,
    GaConnectorLastClickSource_lr,
    GaConnectorLastClickLandingPage_lr,
    GaConnectorLastClickChannel_lr,
    GaConnectorFirstClickChannel_lr,
    GaConnectorFirstClickMedium_lr,
    GaConnectorFirstClickSource_lr,
    MarketingCampaign_lr,
    MarketingChannel_lr,
    ClickSource_lr,
    AllTrafficSources_lr,
    Coverage_lr,
    CASE
        WHEN CONVERT(VARCHAR(8), CreatedDateTimeLeadReq_lr, 108) BETWEEN '09:00:00' AND '19:00:00'
            THEN 1
        ELSE 0
    END AS WithinTimeWindow,
    ROW_NUMBER() OVER (PARTITION BY CompanyName_lr  ORDER BY CreatedDateTimeLeadReq_lr desc) AS seq
  FROM [careabout-db].dbo.ViewLeadRequestMod

  WHERE CreatedDateTimeLeadReq_lr BETWEEN '2025-05-13 00:00:00.000' AND '2025-05-13 23:59:59.000'
	AND HcAssessmentStage_lr IN ('Newly Funded','Switching','Newly Funded - Scheduled')
    AND Coverage_lr = 'Coverage'
     AND Status_lr NOT IN ('Duplicate', 'Invalid details')
     AND (CampaignName_lr IN ('Homecare All','Hunting Bunnies','Phone','Home Care back-up') or (CampaignName_lr IS NULL and LeadType_lr = 'Phone'))
     AND Object_lr = 'Lead'
  
)

--,LeadRequestMod_dedupe as (
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
    Email_lr,
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
    WhoNeedsHelp_lr, 
    GaConnectorLastClickCampaign_lr,
    GaConnectorLastClickSource_lr,
    GaConnectorLastClickLandingPage_lr,
    GaConnectorLastClickChannel_lr,
    GaConnectorFirstClickChannel_lr,
    GaConnectorFirstClickMedium_lr,
    GaConnectorFirstClickSource_lr,
    MarketingCampaign_lr,
    MarketingChannel_lr,
    ClickSource_lr,
    AllTrafficSources_lr,
    Coverage_lr,
   WithinTimeWindow
FROM LeadRequestMod
WHERE seq = 1
