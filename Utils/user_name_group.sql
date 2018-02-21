  SELECT   DISTINCT f_torzsszam, f_nev, f_kiscsoport
    FROM   (  SELECT   DISTINCT
                       f_nev,
                       f_csoport,
                       f_login,
                       f_torzsszam,
                       f_kiscsoport,
                       CASE
                          WHEN NOT EXISTS
                                  (SELECT   f_csoportid
                                     FROM      t_user t1
                                            JOIN
                                               t_csoport
                                            USING (f_csoportid)
                                    WHERE   f_szervezet = 'AFC'
                                            AND f_csoport IN
                                                     ('AFC/Operatív',
                                                      'AFC/Komplex',
                                                      'AFC/Egyedi')
                                            AND t1.f_torzsszam = a.f_torzsszam)
                          THEN
                             0
                          ELSE
                             1
                       END
                          AS uj_csoportjogvan
                FROM            t_user a
                             JOIN
                                t_csoport b
                             USING (f_csoportid)
                          LEFT OUTER JOIN
                             r_user_kiscsoport c
                          ON (a.f_userid = c.f_userid)
                       LEFT OUTER JOIN
                          t_user_kiscsoport d
                       USING (f_kiscsoportid)
               WHERE   b.f_szervezet = 'AFC' AND a.f_enabled = 1
            ORDER BY   f_nev, f_csoport)
   WHERE   f_kiscsoport IS NOT NULL AND f_kiscsoport <> '006033T'
ORDER BY   f_nev