CREATE OR REPLACE PROCEDURE POORJ.al_il_perf
IS
BEGIN

EXECUTE IMMEDIATE 'TRUNCATE TABLE t_al_il_telj';

   COMMIT;
   INSERT INTO t_al_il_telj   (F_IVK,
                                F_LEAN_TIP,
                                F_INT_BEGIN,
                                F_INT_END,
                                CSOPORT,
                                LOGIN,
                                TORZSSZAM,
                                NEV,
                                TEVEKENYSEG,
                                F_OKA,
                                CKLIDO)
     SELECT   a.f_ivk,
         x.f_lean_tip,
         a.f_int_begin,
         a.f_int_end,
         kontakt.basic.get_userid_kiscsoport (a.f_userid) AS csoport,
         UPPER (kontakt.basic.get_userid_login (a.f_userid)) AS login,
         TO_NUMBER (kontakt.basic.get_userid_torzsszam (a.f_userid))
            AS torzsszam,
         kontakt.basic.get_userid_nev (a.f_userid) AS nev,
         CASE
            WHEN (   a.f_alirattipusid BETWEEN 1896 AND 1930
                  OR a.f_alirattipusid BETWEEN 1944 AND 1947
                  OR a.f_alirattipusid IN ('1952', '2027', '2028', '2021'))
            THEN
               kontakt.basic.get_alirattipusid_alirattipus (
                  a.f_alirattipusid
               )
            ELSE
               'Egyéb iraton'
         END
            AS tevekenyseg,
         f_oka,
         (a.f_int_end - a.f_int_begin) * 1440 AS cklido
  FROM   afc.t_afc_wflog_lin2 a,
         kontakt.t_lean_alirattipus x,
         kontakt.t_ajanlat_attrib b
 WHERE   a.f_int_begin BETWEEN CASE
                                  WHEN EXTRACT (MONTH FROM SYSDATE) < 7
                                  THEN
                                     TRUNC (SYSDATE, 'Y')
                                  ELSE
                                     ADD_MONTHS (TRUNC (SYSDATE, 'Y'), 6)
                               END
                           AND  TRUNC (SYSDATE, 'DDD')
         AND (a.f_int_end - a.f_int_begin) * 1440 < 45
         AND (a.f_int_end - a.f_int_begin) * 86400 > 1
         AND afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) IS NOT NULL
         AND a.f_ivk = b.f_ivk(+)
         AND a.f_alirattipusid = x.f_alirattipusid
         AND UPPER (kontakt.basic.get_userid_login (a.f_userid)) NOT IN
                  ('MARKIB', 'SZERENCSEK')
         AND x.f_lean_tip = 'AL'
         AND b.f_termcsop IS NOT NULL
UNION
SELECT   a.f_ivk,
         x.f_lean_tip,
         a.f_int_begin,
         a.f_int_end,
         kontakt.basic.get_userid_kiscsoport (a.f_userid) AS csoport,
         UPPER (kontakt.basic.get_userid_login (a.f_userid)) AS login,
         TO_NUMBER (kontakt.basic.get_userid_torzsszam (a.f_userid))
            AS torzsszam,
         kontakt.basic.get_userid_nev (a.f_userid) AS nev,
         kontakt.basic.get_alirattipusid_alirattipus (c.f_alirattipusid)
            AS tevekenyseg,
         f_oka,
         (a.f_int_end - a.f_int_begin) * 1440 AS time
  FROM   afc.t_afc_wflog_lin2 a,
         kontakt.t_lean_alirattipus x,
         kontakt.t_afcil_attrib c
 WHERE   a.f_int_begin BETWEEN CASE
                                  WHEN EXTRACT (MONTH FROM SYSDATE) < 7
                                  THEN
                                     TRUNC (SYSDATE, 'Y')
                                  ELSE
                                     ADD_MONTHS (TRUNC (SYSDATE, 'Y'), 6)
                               END
                           AND  TRUNC (SYSDATE, 'DDD')
         AND (a.f_int_end - a.f_int_begin) * 1440 < 45
         AND a.f_ivk = c.f_ivk(+)
         AND (a.f_int_end - a.f_int_begin) * 86400 > 1
         AND afc.afc_wflog_intezkedes (a.f_ivkwfid, a.f_logid) IS NOT NULL
         AND a.f_alirattipusid = x.f_alirattipusid
         AND UPPER (kontakt.basic.get_userid_login (a.f_userid)) NOT IN
                  ('MARKIB', 'SZERENCSEK')
         AND f_lean_tip = 'IL';
END al_il_perf;
/
