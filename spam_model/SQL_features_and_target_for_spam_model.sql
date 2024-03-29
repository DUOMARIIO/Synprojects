  -- Данные, которые известны на момент создания заявки (именно в этот момент мы хотим проставлять статус потенциального спама)
 -- 1. Данные из формы-заявки - Телефон, почта, имя
 -- 2. URL, LAND, utm-метки, IP
 -- 3. История заявок с этого IP (количество заявок которое оставляется с одного и того же IP за последний x дней)
 -- 4. Производные IP - город, страна
 -- 5. ID сессии. Если человек на одной странице, не обновляет ее и потравляет несоклько заявок, то ID сесии будет тем же самым - UF_CRM_MERGE_LEAD as MERGE_LEAD
 -- 6. Целевая перменная: если статус 'Спам / Spam', то это 1, остальные 0. берем из статуса заявки (таблица DIC_REQUEST в DWH)
 -- 7. Источник лида, берем только 'Веб-сайт / Website' для анализа
 DROP TABLE IF EXISTS ##RAF_temp;
 select distinct c.iD
 , c.DATE_CREATE
 , c.title  -- ИМЯ в форме заявки
,dict_req.NAME AS STATUS_LEAD
,dict_req_par.NAME AS PARENT_STATUS_LEAD
--
,uts_crm_lead.UF_CRM_MERGE_LEAD as MERGE_LEAD
,uts_crm_lead.UF_CRM_1428579860 as NEW_OLD_LEAD
,uts_crm_lead.UF_IP as IP
,uts_crm_lead.UF_CRM_1417767839 as City_IP
,uts_crm_lead.UF_CRM_1417767787 as Country_IP
,uts_crm_lead.UF_CRM_1464341216 as Region_IP
--
, PHONE_1.VALUE as PHONE_MOBILE
, PHONE_2.VALUE as PHONE_WORK
, EMAIL_1.VALUE as EMAIL_HOME
, EMAIL_2.VALUE as EMAIL_WORK
--
into  ##RAF_temp
--
 from [stage].[CRM_b_crm_lead] AS c
	left join [stage].[CRM_b_uts_crm_lead] AS uts_crm_lead ON (c.ID = uts_crm_lead.VALUE_ID)
	left join [stage].[CRM_b_crm_field_multi] AS PHONE_1 ON 
	(c.ID = PHONE_1.ELEMENT_ID 
	and PHONE_1.ENTITY_ID = 'LEAD' and PHONE_1.COMPLEX_ID = 'PHONE_MOBILE'
	and  isNULL(PHONE_1.SIGN_DELETED,0) = 0)
	left join [stage].[CRM_b_crm_field_multi] AS PHONE_2 ON 
	(c.ID = PHONE_2.ELEMENT_ID
	and PHONE_2.ENTITY_ID = 'LEAD' and PHONE_2.COMPLEX_ID = 'PHONE_WORK'
	and  isNULL(PHONE_2.SIGN_DELETED,0) = 0	)
	left join [stage].[CRM_b_crm_field_multi] AS EMAIL_1 ON 
	(c.ID = EMAIL_1.ELEMENT_ID
	and EMAIL_1.ENTITY_ID = 'LEAD'  and EMAIL_1.COMPLEX_ID = 'EMAIL_HOME'
	 and  isNULL(EMAIL_1.SIGN_DELETED,0) = 0)
	left join [stage].[CRM_b_crm_field_multi] AS EMAIL_2 ON 
	(c.ID = EMAIL_2.ELEMENT_ID
	and EMAIL_2.ENTITY_ID = 'LEAD' and EMAIL_2.COMPLEX_ID = 'EMAIL_WORK'
	and  isNULL(EMAIL_2.SIGN_DELETED,0) = 0)
	inner JOIN [dbo].[DIC_REQUEST] AS r ON (c.ID = r.CODE)
	-- Словарь - таблица содержит информацию по статусам лидов
	left join [dbo].[DIC_STATUS_REQUEST] AS dict_req ON (r.ID_STATUS_REQUEST = dict_req.ID_STATUS_REQUEST)
	-- Словарь - таблица содержит информацию по статусам лидов (для подтягивания группы статуса заявки)
	left join [dbo].[DIC_STATUS_REQUEST] AS dict_req_par ON (dict_req.PARENT_ID = dict_req_par.ID_STATUS_REQUEST)
	-- источник лида
 	left join [dbo].[ass_request_source_crm] as req_source_crm on r.id_request = req_source_crm.id_request
	left join [dbo].[DIC_SOURCE_CRM] AS source_crm on req_source_crm.ID_SOURCE_CRM = source_crm.ID_SOURCE_CRM
	-- телефоны с формы
	left join stage.CRM_b_crm_field_multi as field_multi on c.ID  = field_multi.ELEMENT_ID
 where 1 = 1
 -- 
 and c.DATE_CREATE >= '2021-01-01' -- Дату можно вывести в переменную для удобства
 and source_crm.NAME = 'Веб-сайт / Website'
 and isNULL(r.SIGN_DELETED,0) = 0;
--------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------
-- Есть очень малая часть заявок, где дублируется по одному и тому же id лида (разные номера или e-mail). разобраться почему так
-- от Неша инфа что менеджер может руками поправить инфу, значит надо найти именно такое состояние полей которое получается сразу после оставления заявки.
-- Убираем статусы 'Не обработан / NEW' так как тут нет еще ясности спам или нет
-- ##RAF_temp_for_SPAM_MODEL - таблица, которая затем постобрабатывается в Python (feature engineering)
 DROP TABLE IF EXISTS ##RAF_temp_for_SPAM_MODEL;
--
select
  A.iD
, case when STATUS_LEAD = 'Спам / Spam' then 1
else 0 end SPAM_flag
, DATEPART(DW, A.DATE_CREATE) as "Day_Of_Week"
, DATEPART(HOUR, A.DATE_CREATE) as "HOUR"
, case when PHONE_MOBILE is not null then PHONE_MOBILE else PHONE_WORK end PHONE
, case when EMAIL_HOME is not null then EMAIL_HOME else EMAIL_WORK end EMAIL
, A.DATE_CREATE
, A.title
, A.MERGE_LEAD
, A.NEW_OLD_LEAD
, A.IP
, A.City_IP
, A.Country_IP
, A.Region_IP
into ##RAF_temp_for_SPAM_MODEL
from ##RAF_temp as A
where iD not in (
select iD
from ##RAF_temp
group by iD
having count(*) > 1
)
and STATUS_LEAD <> 'Не обработан / NEW';
--------------------------------------------------------------------------------------------------------------------------------