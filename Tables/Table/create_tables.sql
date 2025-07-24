-- Script to create all tables for BeeOne project
-- Order: DIMENSIONS → STAGING → FACT

/*==============================
  DIMENSION TABLES
==============================*/
CREATE OR REPLACE TABLE dim_personnel (
    id_personnel         NUMBER PRIMARY KEY,
    matricule_personnel  VARCHAR(50),
    nom_personnel        VARCHAR(100),
    prenom_personnel     VARCHAR(100),
    date_embauche        DATE,
    salaire_base         FLOAT,
    categorie_personnel  VARCHAR(100)
);

CREATE OR REPLACE TABLE dim_operation (
    id_operation        NUMBER PRIMARY KEY,
    reference_operation VARCHAR(200),
    famille_operation   VARCHAR(200)
);

CREATE OR REPLACE TABLE dim_parcelle (
    id_parcelle           NUMBER PRIMARY KEY,
    parcelleculturale     VARCHAR(100),
    reference_parcelle_2  VARCHAR(100),
    annee_plantation      NUMBER(4,0),
    date_plantation       DATE,
    statut                VARCHAR(50),
    superficie_ha         FLOAT,
    date_arrachage        DATE,
    societe               VARCHAR(200),
    ferme                 VARCHAR(200),
    zone                  VARCHAR(100),
    variete               VARCHAR(100),
    culture               VARCHAR(100),
    porte_greffe          VARCHAR(100),
    groupe_operationnel   VARCHAR(100),
    tokenpolygone         VARCHAR(1000),
    latitude              FLOAT,
    longitude             FLOAT
);

CREATE OR REPLACE TABLE dim_ferme (
    ferme       VARCHAR(200) PRIMARY KEY,
    date        DATE,
    famille     VARCHAR(100),
    Oper_liste  VARCHAR(100),
    ouvrier     NUMBER,
    HJ          FLOAT,
    Categorie   VARCHAR(100),
    sup_ferme   FLOAT
);

CREATE OR REPLACE TABLE dim_centre (
    IDCentre_Intermediaire NUMBER PRIMARY KEY,
    Titre                  VARCHAR(200),
    Description            VARCHAR(500),
    ID_Service             NUMBER,
    Type_centre            VARCHAR(100),
    Code_Campagne          VARCHAR(50),
    ID_Responsable         NUMBER,
    Date_debut             DATE,
    Date_fin               DATE,
    UO                     VARCHAR(50),
    Cout_pre_etabli        FLOAT,
    created                DATE,
    updated                DATE,
    updatedBy              VARCHAR(100),
    Section                VARCHAR(100),
    Reference              VARCHAR(100),
    Ref_titre              VARCHAR(100),
    IDsociete              NUMBER,
    is_source              BOOLEAN,
    CreatedBy              VARCHAR(100)
);

CREATE OR REPLACE TABLE dim_campagne (
    id_campagne        NUMBER PRIMARY KEY,
    date_debut         DATE,
    date_fin           DATE,
    societe            VARCHAR(200),
    reference_campagne VARCHAR(100),
    id_campagne_n1     NUMBER REFERENCES dim_campagne(id_campagne)
);

CREATE OR REPLACE TABLE dim_calendar (
    date_cal       DATE PRIMARY KEY,
    "Year"         NUMBER,
    "Quarter"      VARCHAR(10),
    "Month Number" NUMBER,
    "Month"        VARCHAR(20),
    "Day"          NUMBER,
    "Week of Year" NUMBER,
    "Day of Week"  NUMBER,
    "Day Name"     VARCHAR(20)
);

/*==============================
  STAGING TABLES
==============================*/
CREATE OR REPLACE TABLE COMPTES_BUDGETAIRES (
    id_code_budgetaire NUMBER PRIMARY KEY,
    classe_            VARCHAR(50),
    rubrique_1         VARCHAR(100),
    rubrique_2         VARCHAR(100),
    rubrique_3         VARCHAR(100),
    rubrique_4         VARCHAR(100),
    rubrique_5         VARCHAR(100),
    code_budgetaire    VARCHAR(100),
    unite              VARCHAR(50),
    cout_unitaire      FLOAT,
    compte_pl          NUMBER
);

CREATE OR REPLACE TABLE COMPTES_ANALYTIQUES (
    id_code_analytique        NUMBER PRIMARY KEY,
    classe_                   VARCHAR(50),
    rubrique_1                VARCHAR(100),
    rubrique_2                VARCHAR(100),
    rubrique_3                VARCHAR(100),
    rubrique_4                VARCHAR(150),
    rubrique_5                VARCHAR(100),
    id_referentiel            NUMBER,
    table_nom                 VARCHAR(100),
    ordre_                    NUMBER,
    type_charge               VARCHAR(50),
    code_analytique           VARCHAR(150),
    id_bdg_attachements_codes NUMBER,
    id_code_budgetaire        NUMBER REFERENCES COMPTES_BUDGETAIRES(id_code_budgetaire)
);

CREATE OR REPLACE TABLE COMPTES_PL (
    id_bdg_comptes_pl  NUMBER PRIMARY KEY,
    code               VARCHAR(100),
    type               VARCHAR(50),
    ordre              NUMBER,
    formule            VARCHAR(255),
    creation_par       VARCHAR(100),
    creation_date      DATE,
    niveau             NUMBER,
    id_compte_parent   NUMBER REFERENCES COMPTES_PL(id_bdg_comptes_pl)
);

CREATE OR REPLACE TABLE PROFIL_DE_PRODUCTION (
    id_bdg_profil_production NUMBER PRIMARY KEY,
    id_campagne              NUMBER REFERENCES dim_campagne(id_campagne),
    profil                   VARCHAR(150),
    descriptif               VARCHAR(500),
    filiere                  VARCHAR(100),
    id_parcelleculturale     NUMBER UNIQUE REFERENCES dim_parcelle(id_parcelle),
    parcelle                 VARCHAR(100),
    superficie               FLOAT,
    statut_arrachage         VARCHAR(100),
    variete                  VARCHAR(100),
    culture                  VARCHAR(100),
    ferme                    VARCHAR(200) REFERENCES dim_ferme(ferme),
    date_debut_travaux       DATE,
    previsionnelle           NUMBER(1,0)
);

CREATE OR REPLACE TABLE PRODUCTION_BEEONE (
    idvente             NUMBER PRIMARY KEY,
    ferme               VARCHAR(200) REFERENCES dim_ferme(ferme),
    reference_vente     VARCHAR(150),
    date_vente          DATE,
    total               FLOAT,
    client              NUMBER,
    description         VARCHAR(500),
    date_recolte        DATE,
    prix                FLOAT,
    pfq                 FLOAT,
    idparcelle          NUMBER REFERENCES dim_parcelle(id_parcelle),
    idproduit_rendement NUMBER,
    id_code_analytique  NUMBER REFERENCES COMPTES_ANALYTIQUES(id_code_analytique),
    poids_moyen         FLOAT,
    quantite            FLOAT,
    poids_kg            FLOAT,
    Unite               VARCHAR(50),
    chiffre_affaire     FLOAT
);

CREATE OR REPLACE TABLE COUTS_BEEONE (
    date                DATE,
    charge_niv1         VARCHAR(100),
    charge_niv2         VARCHAR(100),
    charge_niv3         VARCHAR(100),
    charge_article      VARCHAR(150),
    ferme               VARCHAR(200) REFERENCES dim_ferme(ferme),
    culture             VARCHAR(100),
    variete             VARCHAR(100),
    idparcelleculturale NUMBER REFERENCES dim_parcelle(id_parcelle),
    parcelle_culturale  VARCHAR(100),
    cout                FLOAT,
    quantite            FLOAT,
    cout_unitaire       FLOAT,
    compte_analytique   VARCHAR(400)
);

CREATE OR REPLACE TABLE BUDGET (
    id_bdg_versions     NUMBER PRIMARY KEY,
    idparcelle          NUMBER REFERENCES dim_parcelle(id_parcelle),
    idprofil            NUMBER REFERENCES PROFIL_DE_PRODUCTION(id_bdg_profil_production),
    idcomptebudgetaire  NUMBER REFERENCES COMPTES_BUDGETAIRES(id_code_budgetaire),
    montant_total       FLOAT,
    montant_ha          FLOAT,
    DateWeek            DATE
);

CREATE OR REPLACE TABLE VERSIONS_BUDGET (
    reference           VARCHAR(100) PRIMARY KEY,
    id_bdg_versions     NUMBER,
    derniere_generation TIMESTAMP_NTZ,
    hypothese           VARCHAR(100),
    status              VARCHAR(50)
);

/*==============================
  FACT TABLE
==============================*/
CREATE OR REPLACE TABLE fact_pointage (
    date_pointage            DATE,
    id_parcelle              NUMBER REFERENCES dim_parcelle(id_parcelle),
    id_personnel             NUMBER REFERENCES dim_personnel(id_personnel),
    id_operation             NUMBER REFERENCES dim_operation(id_operation),
    cost_analytique_direct   FLOAT,
    hj_direct                FLOAT,
    HJ_HS25                  FLOAT,
    HJ_HS50                  FLOAT,
    HJ_HS100                 FLOAT,
    HJ_HSNM                  FLOAT,
    Cout_HSCU                FLOAT,
    HSCU                     FLOAT,
    id_centre_cout           NUMBER REFERENCES dim_centre(IDCentre_Intermediaire),
    cost_analytique_indirect FLOAT,
    hj_indirect              FLOAT,
    type_affectation         VARCHAR(50),
    Paie_Generee             VARCHAR(50),
    campagne                 NUMBER REFERENCES dim_campagne(id_campagne),
    SBI                      FLOAT,
    cost_analytique          FLOAT,
    cost_paie                FLOAT,
    ferme                    VARCHAR(200) REFERENCES dim_ferme(ferme),
    Oper_liste               VARCHAR(100),
    ouvrier                  NUMBER
);