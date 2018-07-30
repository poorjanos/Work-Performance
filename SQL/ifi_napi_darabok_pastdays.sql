/* Formatted on 2018.03.05. 13:05:30 (QP5 v5.115.810.9015) */
SELECT   a.user_azon,
         c.torzsszam,
         a.szlaforg_fej_azon,
         a.forgtet_azon,
         a.azon_forgtet_azon,
         TO_CHAR (b.ceg_szlaszam) ceg_szlaszam,
         a.azon_forgtet_mod_idopont datum
  FROM   szlaforg_tetel_azon_log a,
         szlaforg_tetel_azon_mind b,
         U_USER_BASE$ c
 WHERE       a.szlaforg_fej_azon = b.szlaforg_fej_azon
         AND a.user_azon = c.user_azon(+)
         AND a.forgtet_azon = b.forgtet_azon
         AND a.azon_forgtet_azon = b.azon_forgtet_azon
         AND TRUNC (a.azon_forgtet_mod_idopont, 'ddd') =
               TRUNC (SYSDATE, 'ddd') - 3
         AND a.allapot_azon_feldolg = '67'
         AND b.forgtet_irany = 'J'
         AND a.user_azon IN
                  ('CSIZMAZIAA',
                   'KOVESIA',
                   'FELLNERH',
                   'MOLNARAM',
                   'KISSAT',
                   'FILATZNM',
                   'HORVATHANDR',
                   'KERTESZA',
                   'HAKLARV',
                   'NEMETHDI',
                   'VISEGRADIM',
                   'KOROCZKINESZJ',
                   'MIHOCZANEKZS',
                   'KUNJ',
                   'PAPPNEKA',
                   'KOZMAGY',
                   'SOGORPV',
                   'SZABONSZ',
                   'GALEM',
                   'KOA',
                   'HORVATHADR',
                   'TAMASIV',
                   'SZABONAS',
                   'RAGATSR',
                   'NEMETHE',
                   'HORVATHESZT',
                   'NEMETHBR',
                   'MEDGYASR',
                   'PFEIFFERNERB',
                   'ROSTAL',
                   'VINKOVITSNEKK',
                   'KOVACDA')