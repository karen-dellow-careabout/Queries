ALTER VIEW vw_ContactRateAnalysis AS

WITH LeadRequestMod AS (
    SELECT 
        Object_lr,
        LeadReqId_lr,
        CreatedDateTimeLeadReq_lr,
        CAST(CreatedDateTimeLeadReq_lr AS date) AS CreatedDateOnly,

     CASE
    WHEN DATENAME(WEEKDAY, CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS DATE)) = 'Monday'
         AND CAST(CreatedDateTimeLeadReq_lr AS date) IN (
             CAST(DATEADD(DAY, -2, CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS DATE)) AS date),
             CAST(DATEADD(DAY, -1, CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS DATE)) AS date)
         )
    THEN 1
    WHEN DATENAME(WEEKDAY, CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS DATE)) != 'Monday'
         AND CAST(CreatedDateTimeLeadReq_lr AS date) = 
             CAST(DATEADD(DAY, -1, CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time' AS DATE)) AS date)
    THEN 1
    ELSE 0
END AS YesterdaysContacts,
CASE
    WHEN CAST(CreatedDateTimeLeadReq_lr AS date) >= 
         CAST(DATETRUNC(MONTH, SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time') AS DATE)
        AND CAST(CreatedDateTimeLeadReq_lr AS date) < 
         CAST(DATEADD(MONTH, 1, DATETRUNC(MONTH, SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time')) AS DATE)
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
        CONVERT(VARCHAR(8), CreatedDateTimeLeadReq_lr, 108) AS TimeOfCreation,

        CASE
            WHEN CONVERT(VARCHAR(8), CreatedDateTimeLeadReq_lr, 108) BETWEEN '09:00:00' AND '19:00:00' THEN 1
            ELSE 0
        END AS WithinTimeWindow

    FROM [careabout-db].dbo.ViewLeadRequestMod
    WHERE 
        HcAssessmentStage_lr IN ('Newly Funded', 'Switching', 'Newly Funded - Scheduled')
        AND Coverage_lr = 'Coverage'
        AND Status_lr NOT IN ('Duplicate', 'Does not qualify', 'Invalid details')
        AND ProductInterest_lr = 'Home Care'
        AND HCPLevel_lr NOT IN ('CHSP', 'Unknown')
        AND LastName_lr != 'test'
        AND FirstName_lr != 'test'
        AND ((Email_lr NOT LIKE '%\@careabout%' AND Email_lr NOT LIKE '%\@test%') OR Email_lr IS NULL)
         and CAST(CreatedDateTimeLeadReq_lr AS date)  >= '2025-05-01'
),

FirstContactLeadsReq AS (
    SELECT 
        LeadId AS LeadRequestId,
        CreatedDateAuTimeZone AS ContactDateTime_AEST,
        CAST(CreatedDateAuTimeZone AS DATE) AS ContactDate_AEST,
        NewValue,
        ROW_NUMBER() OVER (PARTITION BY LeadId, CreatedDate ORDER BY CreatedDate ASC) AS seq
    FROM [careabout-db].dbo.LeadHistory
    WHERE Field = 'Status' AND NewValue IN ('Qualified', 'Do not call', 'Does not qualify')

    UNION ALL

    SELECT 
        ParentId AS LeadRequestId,
        CreatedDateAuTimeZone AS ContactDateTime_AEST,
        CAST(CreatedDateAuTimeZone AS DATE) AS ContactDate_AEST,
        NewValue,
        ROW_NUMBER() OVER (PARTITION BY ParentId, CreatedDate ORDER BY CreatedDate ASC) AS seq
    FROM [careabout-db].dbo.RequestHistory
    WHERE Field = 'Request_Status__c' AND NewValue IN ('Closed - Actioned')
),

FirstContactLeadsReq_dedupe AS (
    SELECT 
        LeadRequestId,
        ContactDateTime_AEST,
        ContactDate_AEST,
        NewValue
    FROM FirstContactLeadsReq
    WHERE seq = 1
)


SELECT 
    lr.*,

    CASE
    WHEN CONVERT(VARCHAR(8), CreatedDateTimeLeadReq_lr, 108) BETWEEN '09:00:00' AND '19:00:00' and Status_lr in ('Qualified','Do not call','Closed - Actioned')
            THEN 1
        ELSE 0
    END AS ContactedWithinDay,
       


    CASE 
        WHEN CAST(CreatedDateTimeLeadReq_lr AS date) >= 
             CAST(DATEADD(DAY, 1 - DAY(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time'), 
                 SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time') AS date)
             AND CAST(CreatedDateTimeLeadReq_lr AS date) < 
             CAST(DATEADD(MONTH, 1, 
                 DATEADD(DAY, 1 - DAY(SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time'), 
                     SYSDATETIMEOFFSET() AT TIME ZONE 'UTC' AT TIME ZONE 'AUS Eastern Standard Time')) AS date)
             AND Status_lr IN ('Qualified', 'Do not call', 'Closed - Actioned') 
        THEN 1
        ELSE 0
    END AS ContactedWithinMonth,
    FORMAT(CreatedDateTimeLeadReq_lr, 'dddd') AS DayName,
    lh.ContactDateTime_AEST,
    lh.ContactDate_AEST,
    lh.NewValue,

    DATEDIFF(MINUTE, CreatedDateTimeLeadReq_lr, ContactDateTime_AEST) AS MinutesDifference,

    CASE
        WHEN DATEDIFF(MINUTE, CreatedDateTimeLeadReq_lr, ContactDateTime_AEST) <= 10 THEN 'Within 10 Mins'
        WHEN DATEDIFF(MINUTE, CreatedDateTimeLeadReq_lr, ContactDateTime_AEST) > 10 
             AND DATEDIFF(MINUTE, CreatedDateTimeLeadReq_lr, ContactDateTime_AEST) <= 30 THEN 'Within 30 Mins'
        WHEN DATEDIFF(MINUTE, CreatedDateTimeLeadReq_lr, ContactDateTime_AEST) > 30 
             AND DATEDIFF(MINUTE, CreatedDateTimeLeadReq_lr, ContactDateTime_AEST) <= 60 THEN 'Within 60 Mins'
        WHEN DATEDIFF(MINUTE, CreatedDateTimeLeadReq_lr, ContactDateTime_AEST) > 60 THEN 'More than hour'
        ELSE 'NULL'
    END AS TimeToCall

FROM LeadRequestMod lr
LEFT JOIN FirstContactLeadsReq_dedupe lh
    ON lr.LeadReqId_lr = lh.LeadRequestId
    AND lr.CreatedDateOnly >= lh.ContactDate_AEST

GROUP BY 
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
    ContactDateTime_AEST,
    ContactDate_AEST,
    NewValue
  
    
