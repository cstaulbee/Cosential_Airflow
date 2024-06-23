-- Declare the model name and schema
{{ config(
    materialized='table',
    schema='cosential',
    alias='stg_cosential_opportunities'
) }}

-- Define the SQL query to extract data from the Azure SQL Server source
SELECT
    opp.ClientId AS ClientId,
    opp.OpportunityName AS OpportunityName,
    opp.OpportunityId AS OpportunityId,
    opp.Cost AS Cost,
    opp.Stage AS Stage,
    opp.EstimatedStartDate AS EstimatedStartDate,
    opp.EstimatedCompletionDate AS EstimatedCompletionDate,
    opp.ClientName AS ClientName,
    opp.EstimatedFeePercentage AS EstimatedFeePercentage,
    opp.Size AS Size,
    opp.Score AS Score,
    opp.State AS State,
    opp.Probability AS Probability,
    opp.ProjectProbability AS ProjectProbability,
    opp.CloseDate AS CloseDate,
    opp.OpportunityValueListID2 AS OpportunityValueListID2,
    opp.OpportunityValueListID3 AS OpportunityValueListID3,
    opp.OpportunityValueListID4 AS OpportunityValueListID4,
    opp.OpportunityValueListID5 AS OpportunityValueListID5,
    opp.OpportunityShortText3 AS PreconLead,
    opp.LastModifiedDateTime AS LastModifiedDateTime,
    opp.OpportunityNumber AS OpportunityNumber,
    opp.OpportunityShortText2 AS ProjectDirector,
    opp.GrossMarginPercentSTD AS FeeTarget,
    opp.MarketingCostBudget AS PursuitCostBudget,
    opp.FeePercent AS GNGOFTMinimum,
    opp.Markup AS GNGOFTTarget,
    opp.Comments AS Architect,
    opp.QualsDueDate AS GNGMeetingDate,
    opp.FactoredCostSTD AS CaptureGoal,
    opp.fundProbability AS AwardedOFT,
    opp.OpportunityValueListID1 AS OpportunityValueListID1,
    opp.Address1 AS Address1,
    opp.City AS City,
    opp.PostalCode AS PostalCode,
    opp.County AS County,
    dm.DeliveryMethodName AS DeliveryMethod,
    ofc.OfficeName AS OfficeName,
    pc.CategoryName AS CategoryName,
    pt.ProspectTypeName AS ProspectTypeName,
    r.RoleName AS Market,
    st.SubmittalTypeName AS SolicitationType
FROM 
    {{ source('azure_sql_server', 'Cosential_Opportunities') }} opp
LEFT JOIN 
    {{ source('azure_sql_server', 'Cosential_Opportunities_DeliveryMethod') }} dm
ON
    opp.OpportunityId = dm.ObjectId
LEFT JOIN 
    {{ source('azure_sql_server', 'Cosential_Opportunities_Office') }} ofc
ON
    opp.OpportunityId = ofc.ObjectId
LEFT JOIN 
    {{ source('azure_sql_server', 'Cosential_Opportunities_PrimaryCategory') }} pc
ON
    opp.OpportunityId = pc.ObjectId
LEFT JOIN 
    {{ source('azure_sql_server', 'Cosential_Opportunities_ProspectType') }} pt
ON  
    opp.OpportunityId = pt.ObjectId
LEFT JOIN 
    {{ source('azure_sql_server', 'Cosential_Opportunities_Role') }} r
ON
    opp.OpportunityId = r.ObjectId
LEFT JOIN 
    {{ source('azure_sql_server', 'Cosential_Opportunities_SubmittalType') }} st
ON
    opp.OpportunityId = st.ObjectId
