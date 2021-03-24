-- Query 1144 DocDebit - List
--with transactor as (select /*+ materialize */  * from v_td_dic_transactor)
SELECT dd.rowid/* антиODACпарсинг*/,
       d.ENTITYTYPE_ID,
       dd.id,
       d.document_number,
       d.document_number as CODE,
       d.document_date,

       --wc.CODE_FULL AS client_code,
       vdc.CODE AS client_code,

       --tr.name ||' '|| opf.tag as FULL_CLIENT,
       vdc.NAME ||' '|| vdc.OPF_TAG as FULL_CLIENT,

       --ds.code ||' '|| ds.Name as TRANSACTOR_NAME,  -- Код и Наименование РТТ
       vdc.SHOP_CODE ||' '|| vdc.SHOP_NAME as TRANSACTOR_NAME,  -- Код и Наименование РТТ

       --decode(wc.accord_form_id, 211, tr.through_whom, '') as through_whom,
       decode(vdc.accord_form_id, 211, vdc.THROUGH_WHOM, '') as through_whom,

       decode((SELECT COUNT(D.ID)
               FROM DOC_DEBIT_RULE_SUPERPASS Drs
               WHERE Drs.DOC_DEBIT_ID = d.id
                 AND Drs.USER_PASS_ID IS NULL),
              0,
              ' ',
              '!') as has_not_passed_rules,
       decode((SELECT COUNT(D.ID)
               FROM DOC_DEBIT_RULE_SUPERPASS Drs
               WHERE Drs.DOC_DEBIT_ID = d.id
                 AND Drs.USER_PASS_ID IS NULL
                 AND drs.rule_id=28),
              0,
              ' ',
              'Да') as IS_RULE28_NOT_PASSED,
       decode(dd.redelivery,1,'Да','Нет') as redelivery,
       d.status_id,
       s.NAME AS status_name,
       dfc.name As contract_name,
       trim(dfc.name||' '||dep.name) as DEPARTMENT_NAME,
       dpa.name as agent_name,
       dd.sum_total,
       nullif(dd.sum_return_tare, 0) as sum_return_tare,
       dnh_r.name as NonDelivery_Reason_Name,
       dd.sum_vat,
       dd.sum_taxinvoice,
       decode(d.entitytype_id,
              175,
              dpf.name,
              5306,
              dpf.name,
              get_fullname_place(dd.warehouse_id)) as Place_name,
       pf.name as Pay_Form_Name,
       dd.delivery_type_id,
       cdt.note as delivery_type_name,
       dd.ppc_order_number,
       decode(dd.is_special_deb,1,et.tag||' СП',et.tag) as doc_tag,
       d.branch_id,
       place.name as branch_name,
       trunc(d.created_date) as create_date,
       to_char(d.created_date,'HH24:MI:SS') as create_time,
       d.firm_id,

       --tr_f.name as firm_name,
       vdf.name as firm_name,

       dap.action_name,
       dd.for_forwarder,
       dd.warehouse_id,
       get_fullname_place(dd.warehouse_id) as warehouse_name,
       dd.is_create_income_028,
       nvl(dd.is_good_weight,0) as is_good_weight,
       afc.name as accord_form_name,

       --TR.OKPO,
       vdc.OKPO,

       u.name as created_by
FROM doc_debit dd,
     doc_document d,
     V_DIC_CLIENT vdc,
     --dic_work_condition wc,
     --transactor tr,
     --dic_organizational_form opf,
     --DIC_SHOP DS,
     --dic_territory ter,
     --elsys_const pay,
     elsys_status s,
     v_dic_person dpa,
     v_dic_person dpf,
     dic_position dpos,
     dic_department dep,
     dic_fin_centre dfc,
     elsys_const pf,
     (SELECT DOC_ID,
             REASON_ID
      FROM (SELECT DH.DOC_ID,
                   DH.REASON_ID,
                   ROW_NUMBER() OVER(PARTITION BY DH.DOC_ID ORDER BY DH.DATE_STATUS_CHANGE DESC) AS RNUM
            FROM DOC_NONDELIVERY_HISTORY DH)
      WHERE RNUM <= 1) DNH,
     dic_nondelivery dnh_r,
     dic_place place,
     --transactor tr_f,
     dic_action_price dap,
     elsys_const cdt,
     elsys_type et,
     elsys_const afc,
     elsys_user u,
     v_dic_firm vdf
WHERE dd.id = d.ID
  AND dd.WORK_CONDITION_ID = vdc.ID
  AND d.status_id = s.ID
  and dd.agent_id = dpa.id(+)
  and dd.agent_position_id = dpos.id(+)
  and dd.forwarder_person_id = dpf.id(+)
  and dd.pay_form_id = pf.id(+)
  and dnh.doc_id(+) = dd.id
  and dnh_r.id(+) = dnh.reason_id
  and dep.id(+) = dpos.department_id
  and dfc.id(+) = dpos.contract_id
  and d.branch_id = place.id
  --and tr_f.id = d.firm_id
  and vdf.id = d.firm_id
  and dap.id(+) = dd.action_price_id
  and cdt.id = dd.delivery_type_id
  and et.id = d.entitytype_id
  and afc.id = dd.accord_form_id
  and u.user_id(+) = d.created_by
  AND ((d.document_date) BETWEEN TO_DATE('2021/03/23', 'yyyy/mm/dd') AND TO_DATE('2021/03/30', 'yyyy/mm/dd'))
ORDER BY  d.document_date  DESC ,  d.document_number  DESC 
