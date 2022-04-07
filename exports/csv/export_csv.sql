---------------------------------------------------POPAmphibien standard------------------------------------------
-- View: gn_monitoring.v_export_popamphibien_standard
-- Export avec une entrée observations, permettant de récupérer les occurrences d'observations avec l'ensemble
-- des attributs spécifiques du protocole. Ne renvoie pas les visites sans observations.
-- Version du 7 avril 2022

DROP VIEW IF EXISTS gn_monitoring.v_export_popamphibien_standard;

CREATE OR REPLACE VIEW gn_monitoring.v_export_popamphibien_standard AS
SELECT
    -- identifiant unique
    o.uuid_observation AS uuid_observation,
    -- Site et variables associées
    s.base_site_name AS nom_site,
    st_x(s.geom_local) AS x_lambert93,
    st_y(s.geom_local) AS y_lambert93,
    alt.altitude_min AS altitude_min,
    alt.altitude_max AS altitude_max,
    ref_nomenclatures.get_nomenclature_label(((sc.data::json #> '{milieu_aquatique}'::text[])::text)::integer,'fr') AS milieu_aquatique,
    ref_nomenclatures.get_nomenclature_label(((sc.data::json #> '{variation_eau}'::text[])::text)::integer,'fr') AS variation_eau,
    ref_nomenclatures.get_nomenclature_label(((sc.data::json #> '{courant}'::text[])::text)::integer,'fr') AS courant,
    com.area_name AS commune,
    string_agg(distinct(sp.area_name)||'('||bat.type_code||')', ', ') AS site_protege,
    -- Informations sur la visite
    v.uuid_base_visit AS uuid_visite,
    v.visit_date_min AS date_visite,
    (vc.data::json #> '{num_passage}'::text[]) AS numero_visite,
    obs.observers,
    ref_nomenclatures.get_nomenclature_label(((vc.data::json #> '{pluviosite}'::text[])::text)::integer,'fr') AS pluviosite,
    ref_nomenclatures.get_nomenclature_label(((vc.data::json #> '{couverture_nuageuse}'::text[])::text)::integer,'fr') AS couverture_nuageuse,
    ref_nomenclatures.get_nomenclature_label(((vc.data::json #> '{vent}'::text[])::text)::integer,'fr') AS vent,
    ref_nomenclatures.get_nomenclature_label(((vc.data::json #> '{turbidite}'::text[])::text)::integer,'fr') AS turbidite,
    ref_nomenclatures.get_nomenclature_label(((vc.data::json #> '{vegetation_aquatique_principale}'::text[])::text)::integer,'fr') AS vegetation_aquatique_principale,
    ref_nomenclatures.get_nomenclature_label(((vc.data::json #> '{rives}'::text[])::text)::integer,'fr') AS rives,
    ref_nomenclatures.get_nomenclature_label(((vc.data::json #> '{habitat_terrestre_environnant}'::text[])::text)::integer,'fr') AS habitat_terrestre_environnant,
    ref_nomenclatures.get_nomenclature_label(((vc.data::json #> '{activite_humaine}'::text[])::text)::integer,'fr') AS activite_humaine,
    v.comments AS commentaire_visite,
    -- Informations sur l'observation
    o.cd_nom AS cd_nom,
    t.lb_nom AS nom_latin, 
    t.nom_vern AS nom_francais,
    ref_nomenclatures.get_nomenclature_label(((oc.data::json #> '{id_nomenclature_typ_denbr}'::text[])::text)::integer, 'fr') AS type_denombrement,
    ((oc.data::json #> '{count_min}'::text[])::text)::integer AS count_min,
    ((oc.data::json #> '{count_max}'::text[])::text)::integer AS count_max,
    ref_nomenclatures.get_nomenclature_label(((oc.data::json #> '{id_nomenclature_stade}'::text[])::text)::integer,'fr') AS stade_vie,
    ref_nomenclatures.get_nomenclature_label(((oc.data::json #> '{id_nomenclature_sex}'::text[])::text)::integer,'fr') AS sexe,
    o.comments AS commentaire_obs
FROM gn_monitoring.t_observations o
JOIN gn_monitoring.t_observation_complements oc ON oc.id_observation = o.id_observation
JOIN gn_monitoring.t_base_visits v ON o.id_base_visit = v.id_base_visit
JOIN gn_monitoring.t_visit_complements vc on v.id_base_visit = vc.id_base_visit 
JOIN gn_monitoring.t_base_sites s ON s.id_base_site = v.id_base_site
JOIN gn_monitoring.t_site_complements sc on sc.id_base_site = s.id_base_site 
JOIN gn_commons.t_modules m ON m.id_module = v.id_module
JOIN taxonomie.taxref t ON t.cd_nom = o.cd_nom
LEFT JOIN ref_geo.l_areas com ON st_intersects(s.geom_local, com.geom)
LEFT JOIN ref_geo.l_areas sp ON st_intersects(s.geom_local, sp.geom)
JOIN ref_geo.bib_areas_types bat ON sp.id_type=bat.id_type
LEFT JOIN LATERAL ( SELECT array_agg(r.id_role) AS ids_observers,
    string_agg(concat(r.nom_role, ' ', r.prenom_role), ' ; '::text) AS observers
    FROM gn_monitoring.cor_visit_observer cvo
    JOIN utilisateurs.t_roles r ON r.id_role = cvo.id_role
    WHERE cvo.id_base_visit = v.id_base_visit) obs ON true
LEFT JOIN LATERAL ref_geo.fct_get_altitude_intersection(s.geom_local) alt(altitude_min, altitude_max) ON true
WHERE com.id_type=(SELECT id_type FROM ref_geo.bib_areas_types WHERE type_code='COM')
AND sp.id_type IN (SELECT id_type FROM ref_geo.bib_areas_types WHERE type_code IN ('ZNIEFF1','ZNIEFF2','ZPS','ZCS','SIC','RNCFS','RNR','RNN','ZC'))
AND m.module_code = 'POP2'
GROUP BY o.uuid_observation, o.cd_nom, t.lb_nom, t.nom_vern, o.comments, oc.data, v.visit_date_min, v.comments, v.uuid_base_visit,
s.base_site_name, sc.data, vc.data, alt.altitude_min, alt.altitude_max, obs.observers, com.area_name, s.geom_local;


---------------------------------------------------POPAmphibien analyses------------------------------------------
-- View: gn_monitoring.v_export_popamphibien_analyses
-- Export avec une entrée visites, permettant d'analyser les données de protocole en listant les visites et les 
-- observations associées, y compris visite sans occurrences.
-- Version du 7 avril 2022

-- View: gn_monitoring.v_export_popamphibien_analyses

--DROP VIEW IF EXISTS gn_monitoring.v_export_popamphibien_analyses;

--CREATE OR REPLACE VIEW gn_monitoring.v_export_popamphibien_analyses AS
--SELECT

