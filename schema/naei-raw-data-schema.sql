-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.dataset_file (
  dataset_file_id integer NOT NULL DEFAULT nextval('dataset_file_dataset_file_id_seq'::regclass),
  dataset_prefix text NOT NULL CHECK (dataset_prefix ~ '^NAEI[0-9]{4}(ds|pv)$'::text),
  file_name text NOT NULL,
  extracted_at timestamp with time zone,
  source_url text,
  CONSTRAINT dataset_file_pkey PRIMARY KEY (dataset_file_id),
  CONSTRAINT dataset_file_dataset_prefix_file_name_key UNIQUE (dataset_prefix, file_name)
);

CREATE TABLE public.naei2023ds_series (
  ds_series_id integer NOT NULL DEFAULT nextval('naei2023ds_series_ds_series_id_seq'::regclass),
  pollutant_id bigint,
  nfr_group_id bigint,
  source_id integer,
  activity_id integer,
  dataset_file_id integer NOT NULL,
  CONSTRAINT naei2023ds_series_pkey PRIMARY KEY (ds_series_id),
  CONSTRAINT naei2023ds_series_pollutant_id_fkey FOREIGN KEY (pollutant_id) REFERENCES public.naei_global_t_pollutant(id),
  CONSTRAINT naei2023ds_series_nfr_group_id_fkey FOREIGN KEY (nfr_group_id) REFERENCES public.naei_global_t_nfrcode(id),
  CONSTRAINT naei2023ds_series_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.naei_global_t_sourcename(id),
  CONSTRAINT naei2023ds_series_activity_id_fkey FOREIGN KEY (activity_id) REFERENCES public.naei_global_t_activityname(id),
  CONSTRAINT naei2023ds_series_dataset_file_id_fkey FOREIGN KEY (dataset_file_id) REFERENCES public.dataset_file(dataset_file_id)
);

CREATE TABLE public.naei2023ds_values (
  ds_series_id integer NOT NULL,
  reporting_year smallint NOT NULL CHECK (reporting_year >= 1900 AND reporting_year <= 2100),
  emission_value double precision,
  CONSTRAINT naei2023ds_values_pkey PRIMARY KEY (ds_series_id, reporting_year),
  CONSTRAINT naei2023ds_values_ds_series_id_fkey FOREIGN KEY (ds_series_id) REFERENCES public.naei2023ds_series(ds_series_id)
);

CREATE TABLE public.naei2024pv_series (
  pv_series_id integer NOT NULL DEFAULT nextval('naei2024pv_series_pv_series_id_seq'::regclass),
  pollutant_id bigint,
  nfr_group_id bigint,
  source_id integer,
  activity_id integer,
  dataset_file_id integer NOT NULL,
  CONSTRAINT naei2024pv_series_pkey PRIMARY KEY (pv_series_id),
  CONSTRAINT naei2024pv_series_pollutant_id_fkey FOREIGN KEY (pollutant_id) REFERENCES public.naei_global_t_pollutant(id),
  CONSTRAINT naei2024pv_series_nfr_group_id_fkey FOREIGN KEY (nfr_group_id) REFERENCES public.naei_global_t_nfrcode(id),
  CONSTRAINT naei2024pv_series_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.naei_global_t_sourcename(id),
  CONSTRAINT naei2024pv_series_activity_id_fkey FOREIGN KEY (activity_id) REFERENCES public.naei_global_t_activityname(id),
  CONSTRAINT naei2024pv_series_dataset_file_id_fkey FOREIGN KEY (dataset_file_id) REFERENCES public.dataset_file(dataset_file_id),
  CONSTRAINT naei2024pv_series_identity_key UNIQUE (dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id)
);

CREATE TABLE public.naei2024pv_values (
  pv_series_id integer NOT NULL,
  reporting_year smallint NOT NULL CHECK (reporting_year >= 1900 AND reporting_year <= 2100),
  metric_label text NOT NULL DEFAULT 'value'::text,
  metric_value double precision,
  CONSTRAINT naei2024pv_values_pkey PRIMARY KEY (pv_series_id, reporting_year, metric_label),
  CONSTRAINT naei2024pv_values_pv_series_id_fkey FOREIGN KEY (pv_series_id) REFERENCES public.naei2024pv_series(pv_series_id)
);

CREATE TABLE public.naei_global_t_activityname (
  id integer GENERATED ALWAYS AS IDENTITY NOT NULL,
  activity_name text,
  CONSTRAINT naei_global_t_activityname_pkey PRIMARY KEY (id),
  CONSTRAINT naei_global_t_activityname_activity_name_key UNIQUE (activity_name)
);

CREATE TABLE public.naei_global_t_nfrcode (
  id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  nfr_code text,
  description text,
  CONSTRAINT naei_global_t_nfrcode_pkey PRIMARY KEY (id),
  CONSTRAINT naei_global_t_nfrcode_nfr_code_key UNIQUE (nfr_code)
);

CREATE TABLE public.naei_global_t_pollutant (
  id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  pollutant text,
  emission_unit text,
  short_pollutant text UNIQUE,
  CONSTRAINT naei_global_t_pollutant_pkey PRIMARY KEY (id),
  CONSTRAINT naei_global_t_pollutant_pollutant_key UNIQUE (pollutant)
);

CREATE TABLE public.naei_global_t_pollutant_alias (
  alias_id bigint GENERATED ALWAYS AS IDENTITY NOT NULL,
  alias_name text NOT NULL,
  alias_key text DEFAULT lower(alias_name) UNIQUE,
  pollutant_id bigint NOT NULL,
  CONSTRAINT naei_global_t_pollutant_alias_pkey PRIMARY KEY (alias_id),
  CONSTRAINT naei_global_t_pollutant_alias_pollutant_id_fkey FOREIGN KEY (pollutant_id) REFERENCES public.naei_global_t_pollutant(id)
);

CREATE TABLE public.naei_global_t_sourcename (
  id integer GENERATED ALWAYS AS IDENTITY NOT NULL,
  source_name text,
  CONSTRAINT naei_global_t_sourcename_pkey PRIMARY KEY (id),
  CONSTRAINT naei_global_t_sourcename_source_name_key UNIQUE (source_name)
);

CREATE TABLE public.unit (
  unit_id integer NOT NULL DEFAULT nextval('unit_unit_id_seq'::regclass),
  unit_name text NOT NULL UNIQUE,
  CONSTRAINT unit_pkey PRIMARY KEY (unit_id)
);
