QUERIES = {
        "COMPTES_ANALYTIQUES": """
            SELECT * FROM bdg_codes_analytiques ca 
            LEFT JOIN bdg_attachements_codes bac on bac.id_code_analytique = ca.id_code_analytique
        """,
        "COMPTES_BUDGETAIRES": """
            SELECT 
                cb.id_code_budgetaire, classe_, rubrique_1, rubrique_2, rubrique_3, 
                rubrique_4, rubrique_5, code_budgetaire, unite, cout_unitaire, pl.id_bdg_comptes_pl as compte_pl 
            FROM bdg_codes_budgetaires cb
            LEFT JOIN bdg_comptes_pl_details pld on pld.id_code_budgetaire = cb.id_code_budgetaire
            LEFT JOIN bdg_comptes_pl pl on pl.id_bdg_comptes_pl = pld.id_compte_pl
        """,
        "PRODUCTION_BEEONE": """
            --BLOC VENTE MARCHE LOCAL
            SELECT 
                v.idfermes, v.idvente, v.ref as reference_vente, 
                CAST(v.date AS DATETIME) AS date_vente,
                v.total as total, v.client, v.description, 
                CAST(recp_c.DATE AS DATETIME) AS date_recolte, 
                recp.pu as prix, CAST(NULL AS DECIMAL(10,2)) as pfq, recp_c_p.ParcelleCulturale as idparcelle,
                pc.idproduit_rendement, ca.code_analytique, ca.id_code_analytique,
                recp_c_p.kg_estime as poids_moyen, recp_c_p.qte_caisse as quantite,
                recp_c_p.kg_estime*recp_c_p.qte_caisse as poids_kg, uo.Unite,
                recp_c_p.qte_caisse * recp_c_p.kg_estime * recp.pu as chiffre_affaire
            FROM vente v
            LEFT JOIN client c on v.client = c.id
            LEFT JOIN Exp_Recp_caisse recp_c on recp_c.IDVente = v.IDVente
            left join Exp_Recp_caisse_ParcelleCulturale recp_c_p on recp_c_p.Exp_Recp_caisse = recp_c.id
            left join parcelleculturale pc on pc.id = recp_c_p.ParcelleCulturale
            left join Bon_Recp_Station recp on recp.Exp_recp_caisse = recp_c.id and pc.variete = recp.idVariete
            LEFT JOIN Unite_Operation uo on uo.IDUnite_Operation = recp_c_p.IDUnite_Operation
            LEFT JOIN bdg_codes_analytiques ca on ca.id_referentiel = pc.idproduit_rendement 
                and table_nom = 'produit_rendement' and rubrique_5 = 'Marché local'
            where recp_c.DATE >= '20230101' and v.type not in (4)
            UNION
            --BLOC VENTE EXPORT
            SELECT 
                v.idfermes, v.idvente, v.ref as reference_vente, 
                CAST(v.date AS DATETIME) AS date_vente,
                v.total as total, v.client, v.description, 
                CAST(rdt_q.Date_Rapport AS DATETIME) AS date_recolte,
                rdt_q.Prix AS prix, rdt_q.PFQ as pfq, rdt_q_p.idparcelle,
                pc.idproduit_rendement, ca.code_analytique, ca.id_code_analytique,
                rdt_q_p.PM as poids_moyen, rdt_q_p.Qte_receptionne as quantite,
                rdt_q_p.Poid_recp as poids_kg, uo.Unite,
                prix*Poid_recp as chiffre_affaire
            FROM RDT_Rapport_qualite rdt_q
            LEFT JOIN vente v on v.IDVente = rdt_q.IDVente
            LEFT JOIN client c on v.client = c.id
            LEFT JOIN RDT_Parcelle_Rapport_qualite rdt_q_p on rdt_q.IDRDT_Rapport_qualite = rdt_q_p.IDRDT_Rapport_qualite
            LEFT JOIN Unite_Operation uo on uo.IDUnite_Operation = rdt_q_p.IDUnite_Operation
            LEFT JOIN parcelleculturale pc on rdt_q_p.idparcelle = pc.id
            LEFT JOIN bdg_codes_analytiques ca on ca.id_referentiel = pc.idproduit_rendement 
                and table_nom = 'produit_rendement' and rubrique_5 = 'Export'
            where rdt_q.Date_Rapport >= '20230101'
        """,
        "PROFIL_DE_PRODUCTION": """
            SELECT DISTINCT 
                ppc.id_bdg_profil_production, 
                ppc.id_campagne as id_campagne,
                pp.designation as Profil, pp.descriptif as Descriptif, 
                ppc.filiere as Filière, pc.id as id_parcelleculturale, 
                pc.ref as Parcelle, pc.Sup as Superficie, 
                pc.Statut_Cycle as Statut_Arrachage, v.variete as Variété, 
                c.Culture, f.nom as Ferme,
                pc.Date_Previsionnelle as datedebuttravaux, pc.previsionnelle
            FROM bdg_parcelles_profils_campagnes ppc
            LEFT JOIN Fermes_Compagne fc on fc.ID_compagne = ppc.id_campagne
            LEFT JOIN bdg_profils_production pp on pp.id_bdg_profil_production = ppc.id_bdg_profil_production
            LEFT JOIN parcelleculturale pc on pc.id = ppc.id_parcelle_culturale
            LEFT JOIN variete v on v.id = pc.variete
            LEFT JOIN culture c on c.id = v.culture
            LEFT JOIN fermes f on f.idfermes = pc.idfermes
            where pc.id is not null
        """,
        "COUTS_BEEONE": """
            select 
                CAST(date AS DATETIME) AS date, 
                Charge_niv1,
                Charge_niv2,
                Charge_niv3,
                Charge_Article,
                f.nom as ferme,
                c.culture,
                v.variete, idparcelleculturale,
                pc.ref as Parcelle_Culturale,
                cout,
                quantite,
                cout_unitaire,
                CONCAT(Charge_niv1,Charge_niv2,Charge_niv3,Charge_Article) as Compte_Analytique
            from (
            --FRAIS GENERAUX--
            SELECT 
            DISTINCT
                    CAST(d.date_depense AS DATETIME) AS date,
                    'Frais généraux' as Charge_niv1,
                    cd.categ_depense as Charge_niv2,
                    p.Designation as Charge_niv3,
                    '' as Charge_Article,
                    pcd.ID_parcelle as idparcelleculturale,
                    pcd.cout,
                    0 as quantite, 0 as cout_unitaire
            FROM Depence d
            INNER JOIN Affectation a on a.IDAffectation = d.IDAffectation
            LEFT JOIN produit p on p.id = d.ID_produit
            LEFT JOIN Categorie_depence cd on p.ID_categorie_depense = cd.ID
            LEFT JOIN Sous_categorie_depensce scd on p.IDSous_categorie_depensce = scd.id
            LEFT JOIN ParcelleCulturale_Depence pcd on pcd.id_depence = d.id_depence
            LEFT JOIN ParcelleCulturale on pcd.ID_parcelle = parcelleculturale.id  
            where d.date_depense >= '20240101'
            UNION ALL
            --MO--
            SELECT 
                        CAST(p.date AS DATETIME) AS date,
                        'Main d''oeuvre' as Charge_niv1,
                        fo.famille as Charge_niv2,
                        fo.famille as Charge_niv3,
                        p.Oper_liste as Charge_Article,
                        ppc.ParcCul_ID as idparcelleculturale,
                        ppc.COUT as Cout,
                        0 as quantite, 0 as cout_unitaire
            FROM pointage p
                    LEFT JOIN Operation_REF oper on oper.OpeRef_Intitule = p.Oper_liste
                    LEFT JOIN Famille_Operation fo on fo.id = oper.Oper_Famille
                    LEFT JOIN Groupe_Operation gop on gop.id = oper.OpeRef_Gr
                    LEFT JOIN Fermes f on f.IDFermes = p.idfermes
                    LEFT JOIN Pointage_ParcelleCulturale ppc on ppc.IDPointage = p.IDPointage
                    LEFT JOIN ParcelleCulturale on ppc.ParcCul_ID = parcelleculturale.id 
                    and p.date >= parcelleculturale.Date_Previsionnelle 
            where p.date >= '20240101'
            UNION ALL
            SELECT 
                CAST(ms.date AS DATETIME) AS date, 'Intrants'  as Charge_niv1, p.Categorie as Charge_niv2, p.Sous_Categorie as Charge_niv3, p.Designation as Charge_Article, 
                msp.ParcelleCulturale as idparcelleculturale, sum(msp.qantite * ms.cmup) as Cout, sum(msp.qantite) as quantite, sum(ms.cmup) as cout_unitaire
            FROM Mouvementstock_ParcelleCulturale msp
            LEFT JOIN mouvement_stock ms ON ms.IDMouvement_stock = msp.Mouvement_stock
            LEFT JOIN Depots dp on dp.iddepots = ms.IDDepots
            LEFT JOIN Produit p on p.id = ms.produit
            LEFT JOIN ParcelleCulturale on  msp.ParcelleCulturale = parcelleculturale.id 
                    and ms.date >= parcelleculturale.Date_Previsionnelle 
            Where ms.date >= '20240101' 
            GROUP BY ms.date, ms.produit, ms.IDMouvement_stock, msp.ParcelleCulturale,p.Categorie, p.Sous_Categorie,p.Designation
                    ) as t 
                LEFT JOIN parcelleculturale pc on pc.id = t.idparcelleculturale 
                LEFT JOIN fermes f on pc.idfermes = f.idfermes
                LEFT JOIN variete v on v.id = pc.variete
                LEFT JOIN culture c on v.culture = c.id
        """,
        "BUDGET": """
            select 
                id_bdg_versions, id_parcelle as idparcelle,  
                id_bdg_profil_production as idprofil,
                id_compte_budgetaire as idcomptebudgetaire, 
                total as montant_total, montant_norme as montant_ha,
                DATEADD(WEEK, semaine - 1, DATEADD(DAY, 1 - DATEPART(WEEKDAY, DATEFROMPARTS(annee, 1, 1)), 
                DATEFROMPARTS(annee, 1, 1))) AS DateWeek
            from bdg_versions_details ver_det
        """,
        "COMPTES_PL": """
            SELECT * FROM bdg_comptes_pl
        """,
        "VERSIONS_BUDGET": """
            SELECT 
                reference, id_bdg_versions, 
                CAST(derniere_generation AS DATETIME) AS derniere_generation, 
                hypothese, status 
            FROM bdg_versions 
            where etat = 1
        """,
    "DIM_PERSONNEL": """
        SELECT 
            pers.id AS id_personnel,
            pers.mat AS matricule_personnel,
            pers.nom AS nom_personnel,
            pers.prenom AS prenom_personnel,
            pers.date_embauche,
            pers.Salaire_Base AS salaire_base,
            cp.Categorie AS categorie_personnel
        FROM personnel pers
        LEFT JOIN Categorie_personnel cp ON cp.id = pers.categorie
    """,

    "DIM_OPERATION": """
        SELECT 
            oper.operef_id AS id_operation,
            oper.operef_intitule AS reference_operation,
            fo.famille AS famille_operation
        FROM operation_ref oper
        LEFT JOIN Famille_Operation fo ON fo.id = oper.Oper_Famille
    """,

    "DIM_PARCELLE": """
        SELECT 
            pc.id AS id_parcelle,
            pc.Ref AS parcelleculturale,
            pc.reference AS reference_parcelle_2,
            YEAR(pc.Dat_Plant) AS annee_plantation,
            pc.dat_plant AS date_plantation,
            CASE WHEN pc.statut_cycle = 1 THEN 'En cours' ELSE 'Arrachée' END AS [statut],
            pc.sup AS superficie_ha,
            pc.dat_arrach AS date_arrachage,
            s.Rais_Social AS societe,
            f.Nom AS ferme,
            f.Ville AS [zone],
            v.variete,
            c.culture,
            pg.Libile AS porte_greffe,
            grp.Intitule AS groupe_operationnel,
            pc.tokenpolygone,
            pc.LatPosition AS latitude,
            pc.LngPosition AS longitude
        FROM parcelleculturale pc
        LEFT JOIN fermes f ON f.IDFermes = pc.idfermes
        LEFT JOIN societe s ON s.ID = f.ID_societe
        LEFT JOIN variete v ON v.id = pc.variete
        LEFT JOIN culture c ON c.id = v.culture
        LEFT JOIN Porte_greffe pg ON pg.IDPorte_greffe = pc.IDPorte_greffe
        LEFT JOIN ParcelleCultural_Groupe_Operationnel grp_parc ON grp_parc.ParcelleCultural = pc.id
        LEFT JOIN GroupeCultural_Operationnel grp ON grp.IDGroupeCultural_Operationnel = grp_parc.GroupeCultural
    """,

    "DIM_FERME": """
        SELECT 
            f.nom AS ferme,
            p.date,
            fo.famille,
            p.Oper_liste,
            pp.Pers_Id AS ouvrier,
            pp.HJ,
            cp.Categorie,
            t.sup_ferme
        FROM pointage p
        LEFT JOIN Personnel_Pointage pp ON p.IDPointage = pp.IDPointage
        LEFT JOIN Operation_REF oper ON oper.OpeRef_Intitule = p.Oper_liste
        LEFT JOIN Famille_Operation fo ON fo.id = oper.Oper_Famille
        LEFT JOIN Fermes f ON f.IDFermes = p.idfermes
        LEFT JOIN personnel pers ON pers.id = pp.Pers_Id
        LEFT JOIN Categorie_personnel cp ON cp.id = pers.categorie
        LEFT JOIN (
            SELECT idfermes, SUM(sup) AS sup_ferme 
            FROM parcelleculturale 
            WHERE variete <> 15
            GROUP BY idfermes
        ) AS t ON t.idfermes = p.IDFermes
    """,

    "DIM_CAMPAGNE": """
        
        SELECT 
            id_compagne AS id_campagne,
            date_debut,
            date_fin,
            idsociete AS societe,
            code_compagne AS reference_campagne,
            LAG(id_compagne) OVER (
                PARTITION BY idsociete 
                ORDER BY date_debut
            ) AS id_campagne_n1
        FROM Fermes_Compagne
    """,

    "DIM_CENTRE": """
        SELECT * FROM Centre_Intermediaire;
    """
    ,
    
    "FACT_POINTAGE": """
  -- ========== VUE POUR FACT_POINTAGE ========== --

WITH nbre_centres AS (
    SELECT idpointage, COUNT(*) AS nbre_centres 
    FROM cc_Pointage 
    GROUP BY idpointage
),
superficie_pointage AS (
    SELECT ppc.idpointage, SUM(pc.sup) AS superficie_pointee 
    FROM parcelleculturale pc 
    INNER JOIN Pointage_ParcelleCulturale ppc ON ppc.ParcCul_ID = pc.ID
    GROUP BY ppc.idpointage
),
sup_fermes AS (
    SELECT f.idfermes, SUM(pc.sup) AS superficie_agrumes_ferme 
    FROM fermes f 
    INNER JOIN parcelleculturale pc ON f.idfermes = pc.idfermes 
    WHERE pc.variete <> 15
    GROUP BY f.idfermes
),
direct_costs AS (
    SELECT 
        p.date AS date_pointage,
        f.nom AS ferme,
        ppc.ParcCul_ID AS id_parcelle,
        pp.Pers_Id AS id_personnel,
        COALESCE(p.operef_id, (SELECT o.OpeRef_Id FROM Operation_REF o WHERE p.oper_liste = o.OpeRef_Intitule)) AS id_operation,
        p.oper_liste AS Oper_liste,
        pp.cout * pc.sup / NULLIF(sp.superficie_pointee, 0) AS cout_direct,
        ppc.Cout * pp.HJ / NULLIF(p.Cout_Totale, 0) / NULLIF(COALESCE(pp.seuil_horaire, 8), 0) AS hj_direct,
        ppc.Cout * pp.HS_25 / NULLIF(p.Cout_Totale, 0) / NULLIF(COALESCE(pp.seuil_horaire, 8), 0) AS HJ_HS25,
        ppc.Cout * pp.HS_50 / NULLIF(p.Cout_Totale, 0) / NULLIF(COALESCE(pp.seuil_horaire, 8), 0) AS HJ_HS50,
        ppc.Cout * pp.HS_100 / NULLIF(p.Cout_Totale, 0) / NULLIF(COALESCE(pp.seuil_horaire, 8), 0) AS HJ_HS100,
        ppc.Cout * pp.HS_NM / NULLIF(p.Cout_Totale, 0) / NULLIF(COALESCE(pp.seuil_horaire, 8), 0) AS HJ_HSNM,
        pp.Cout_HSCU AS Cout_HSCU,
        pp.HSCU AS HSCU,
        CAST(NULL AS INT) AS id_centre_cout,
        CAST(NULL AS DECIMAL(10,2)) AS cout_indirect,
        CAST(NULL AS DECIMAL(10,2)) AS hj_indirect
    FROM pointage p
    INNER JOIN Pointage_ParcelleCulturale ppc ON ppc.IDPointage = p.IDPointage
    LEFT JOIN superficie_pointage sp ON sp.IDPointage = p.IDPointage
    LEFT JOIN Personnel_Pointage pp ON p.IDPointage = pp.IDPointage
    LEFT JOIN parcelleculturale pc ON pc.id = ppc.ParcCul_ID
    LEFT JOIN fermes f ON f.IDFermes = p.IDFermes
),
indirect_costs AS (
    SELECT 
        p.date AS date_pointage,
        f.nom AS ferme,
        CAST(NULL AS INT) AS id_parcelle,
        pp.Pers_Id AS id_personnel,
        COALESCE(p.operef_id, (SELECT o.OpeRef_Id FROM Operation_REF o WHERE p.oper_liste = o.OpeRef_Intitule)) AS id_operation,
        p.oper_liste AS Oper_liste,
        CAST(NULL AS DECIMAL(10,2)) AS cout_direct,
        CAST(NULL AS DECIMAL(10,2)) AS hj_direct,
        CAST(NULL AS DECIMAL(10,2)) AS HJ_HS25,
        CAST(NULL AS DECIMAL(10,2)) AS HJ_HS50,
        CAST(NULL AS DECIMAL(10,2)) AS HJ_HS100,
        CAST(NULL AS DECIMAL(10,2)) AS HJ_HSNM,
        CAST(NULL AS DECIMAL(10,2)) AS Cout_HSCU,
        CAST(NULL AS DECIMAL(10,2)) AS HSCU,
        ccp.IDCentre_Intermediaire AS id_centre_cout,
        CASE WHEN nc.nbre_centres = 0 THEN 0 ELSE pp.cout / NULLIF(nc.nbre_centres, 0) END AS cout_indirect,
        ccp.Cout * pp.HJ / NULLIF(p.Cout_Totale, 0) / NULLIF(COALESCE(pp.seuil_horaire, 8), 0) AS hj_indirect
    FROM pointage p
    INNER JOIN CC_Pointage ccp ON ccp.IDPointage = p.IDPointage
    LEFT JOIN nbre_centres nc ON nc.idpointage = p.IDPointage
    LEFT JOIN Personnel_Pointage pp ON p.IDPointage = pp.IDPointage
    LEFT JOIN Centre_Intermediaire cc ON cc.IDCentre_Intermediaire = ccp.IDCentre_Intermediaire
    LEFT JOIN fermes f ON f.IDFermes = p.IDFermes
)
SELECT 
    date_pointage,
    id_parcelle,
    id_personnel,
    id_operation,
    cout_direct AS cost_analytique_direct,
    hj_direct,
    HJ_HS25,
    HJ_HS50,
    HJ_HS100,
    HJ_HSNM,
    Cout_HSCU,
    HSCU,
    id_centre_cout,
    cout_indirect AS cost_analytique_indirect,
    hj_indirect,
    CASE WHEN id_parcelle IS NOT NULL THEN 'Parcelle' ELSE 'Centre' END AS type_affectation,
    CAST(NULL AS BIT) AS Paie_Generee,
    (SELECT TOP 1 cf.ID FROM Fermes_Compagne cf WHERE date_pointage BETWEEN cf.date_debut AND cf.date_fin) AS campagne,
    CAST(NULL AS VARCHAR(50)) AS SBI,
    COALESCE(cout_direct, 0) + COALESCE(cout_indirect, 0) AS cost_analytique,
    0.0 AS cost_paie,
    ferme,
    Oper_liste,
    id_personnel AS ouvrier
FROM direct_costs
UNION ALL
SELECT 
    date_pointage,
    id_parcelle,
    id_personnel,
    id_operation,
    cout_direct AS cost_analytique_direct,
    hj_direct,
    HJ_HS25,
    HJ_HS50,
    HJ_HS100,
    HJ_HSNM,
    Cout_HSCU,
    HSCU,
    id_centre_cout,
    cout_indirect AS cost_analytique_indirect,
    hj_indirect,
    'Centre',
    CAST(NULL AS BIT) AS Paie_Generee,
    (SELECT TOP 1 cf.ID FROM Fermes_Compagne cf WHERE date_pointage BETWEEN cf.date_debut AND cf.date_fin) AS campagne,
    CAST(NULL AS VARCHAR(50)) AS SBI,
    COALESCE(cout_direct, 0) + COALESCE(cout_indirect, 0) AS cost_analytique,
    0.0 AS cost_paie,
    ferme,
    Oper_liste,
    id_personnel AS ouvrier
FROM indirect_costs


    """,
}

# Primary key definitions for merge/upsert
TABLE_KEYS = {
    "COMPTES_ANALYTIQUES": ["id_code_analytique"],              # maps to `stg_comptes_analytique`
    "COMPTES_BUDGETAIRES": ["id_code_budgetaire"],              # maps to `stg_comptes_budgetaires`
    "PRODUCTION_BEEONE": ["idvente"],                           # maps to `stg_production_beeone`
    "PROFIL_DE_PRODUCTION": ["id_bdg_profil_production"],       # maps to `stg_profil_production`
    "COUTS_BEEONE": ["idparcelleculturale", "date"],            # maps to `stg_couts_beeone`
    "BUDGET": ["id_bdg_versions", "idparcelle"],                # maps to `stg_budget`
    "VERSIONS_BUDGET": ["id_bdg_versions"],                     # maps to `stg_versions_budget`
    "DIM_PERSONNEL": ["id_personnel"],                          # maps to `dim_personnel`
    "DIM_OPERATION": ["id_operation"],                          # maps to `dim_operation`
    "DIM_PARCELLE": ["id_parcelle"],                            # maps to `dim_parcelle`
    "DIM_FERME": ["ferme"],                                     # maps to `dim_ferme`
    "DIM_CAMPAGNE": ["id_campagne"],                            # maps to `dim_campagne`
    "DIM_CENTRE": ["IDCentre_Intermediaire"],                   # maps to `dim_centre`
    "FACT_POINTAGE": ["date_pointage", "id_personnel", "id_operation"],  # optional composite key
}

DATE_COLS = {
    "PRODUCTION_BEEONE" : "date_vente",
    "COUTS_BEEONE"      : "date",    "BUDGET"            : "DateWeek",
    "FACT_POINTAGE"     : "date_pointage",
}

# Simplified: Each query automatically uses its own table name
# Query name = Table name (no explicit mapping needed)