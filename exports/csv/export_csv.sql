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
    com.area_name AS communes,
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


------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
--					VERSION					17/02/2021
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- View: gn_monitoring.v_export_popamphibien_analyses

DROP VIEW IF EXISTS gn_monitoring.v_export_popamphibien_analyses;

CREATE OR REPLACE VIEW gn_monitoring.v_export_popamphibien_analyses
 AS

SELECT
		aire.id_dataset::text,
		aire.id_sites_group as id_aire,
		aire.nom_aire,
		aire.commune,
		aire.categories_paysageres,
		ovs.id as id_site,
		ovs.base_site_name AS nom_site,
		ovs.coordonnee_x as coordonnee_x_lamb93,
		ovs.coordonnee_y as coordonnee_y_lamb93,
		ovs.milieu_aquatique,
		ovs.num_passage AS numero_passage,
		ovs.visit_date_min as date_passage,
		ovs.presence_poisson::text,
		ovs.espece ,
		--case when ovs.observe is not null then ovs.observe else 0 end as observe,
		ovs.stade,
		ovs.comments as remarques


	   FROM ( SELECT
				s.id_sites_group,
				s.id_base_site as id,
				s.base_site_name,
				s.coordonnee_x,
				s.coordonnee_y,
				s.milieu_aquatique,
				ov.id_base_site,
				ov.id_base_visit,
				ov.visit_date_min,
				ov.presence_poisson,
				ov.id_observation,
				ov.id_base_visit_o,
				ov.stade,
				ov.espece  ,
				--ov.observe,
				ov.comments,
				ov.num_passage
			    --ov.espece_p

			   FROM ( SELECT

						v.id_base_site,
						v.id_base_visit,
						v.visit_date_min,
						v.presence_poisson,
						o.id_observation,
						o.id_base_visit_o,
						o.espece,
						--o.observe,
						o.stade,
						o.comments,
						v.num_passage
					    --v.espece_p


					   FROM (
							SELECT obs.id_observation,
								obs.id_base_visit AS id_base_visit_o,
								n.label_fr AS stade,
								replace(obs.sexe::text,'"','') as sexe,
								taxon.lb_nom AS espece,
								obs.comments
							   FROM (
									SELECT ob.id_observation,
										ob.id_base_visit,
										ob.cd_nom,
										ob.comments,
										ob.uuid_observation,
										oc.data -> 'id_nomenclature_stade'::text AS st,
										oc.data -> 'sexe'::text AS sexe
									   FROM gn_monitoring.t_observations ob
										 LEFT JOIN gn_monitoring.t_observation_complements oc ON ob.id_observation = oc.id_observation) obs
								 LEFT JOIN taxonomie.taxref taxon ON obs.cd_nom = taxon.cd_nom
								 LEFT JOIN ref_nomenclatures.t_nomenclatures n ON obs.st::character varying::text = n.id_nomenclature::character varying::text
								 ) o
						 LEFT JOIN (
								   SELECT
									visit.id_base_site,
									visit.id_base_visit,
									visit.visit_date_min,
									visit.presence_poisson,
									visit.num_passage
									   FROM (  SELECT vb.id_base_site,
													vb.id_base_visit,
													vb.visit_date_min,
													vc.data -> 'presence_poisson'::text AS presence_poisson,
													vc.data -> 'num_passage'::text AS num_passage
											   FROM gn_monitoring.t_base_visits vb
											   LEFT JOIN gn_monitoring.t_visit_complements vc ON vb.id_base_visit = vc.id_base_visit) visit

								 ) v ON o.id_base_visit_o = v.id_base_visit) ov
				 INNER JOIN ( SELECT
						site.id_sites_group,
						site.id_base_site,
						site.base_site_name,
						st_x(ST_Centroid(site.geom)) AS coordonnee_x,
						st_y(ST_Centroid(site.geom))AS coordonnee_y,
						n1.label_fr AS milieu_aquatique

					   FROM ( SELECT
								sc.data -> 'milieu_aquatique'::text AS ma,

								sc.id_module,
								sb.id_base_site,
								sb.id_inventor,
								sb.id_digitiser,
								sb.id_nomenclature_type_site,
								sb.base_site_name,
								sb.base_site_description,
								sb.base_site_code,
								sb.first_use_date,
								sb.geom,
								sb.geom_local,
								sb.altitude_min,
								sb.altitude_max,
								sb.uuid_base_site,
								sb.meta_create_date,
								sb.meta_update_date,
								sc.id_sites_group
							   FROM gn_monitoring.t_base_sites sb
							   LEFT JOIN gn_monitoring.t_site_complements sc ON sb.id_base_site = sc.id_base_site) site
						 INNER JOIN (select id_module, module_code from  gn_commons.t_modules WHERE lower(module_code)='popamphibien') m ON site.id_module::character varying::text = m.id_module::character varying::text
						 LEFT JOIN ref_nomenclatures.t_nomenclatures n1 ON site.ma::character varying::text = n1.id_nomenclature::character varying::text
						 ) s ON s.id_base_site = ov.id_base_site) ovs

		 LEFT JOIN ( SELECT
				a.id_dataset,
				a.id_sites_group,
				a.sites_group_name AS nom_aire,
				c.nom_com AS commune,
				n.label_fr AS categories_paysageres
			   FROM ( SELECT t_sites_groups.id_sites_group,
						t_sites_groups.sites_group_name,
						(t_sites_groups.data -> 'commune'::text) ->> 0 AS commune,
						t_sites_groups.data -> 'categories_paysageres'::text AS cp,
						t_sites_groups.data -> 'id_dataset'::text AS id_dataset
					   FROM gn_monitoring.t_sites_groups) a
				 LEFT JOIN ref_nomenclatures.t_nomenclatures n ON a.cp::text = n.id_nomenclature::text
				 LEFT JOIN ref_geo.li_municipalities c ON a.commune::character varying::text = c.insee_com::text) aire ON aire.id_sites_group = ovs.id_sites_group
order by (aire.id_sites_group,ovs.id, ovs.id_base_visit);

