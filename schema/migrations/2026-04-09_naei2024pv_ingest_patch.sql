-- NAEI 2024 PV ingest patch
-- Applies territory support + idempotent upsert constraints.
-- This migration is fail-fast: it raises if duplicate data would block new unique constraints.

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) Fail-fast diagnostics before adding unique constraints.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.dataset_file
    GROUP BY dataset_prefix, file_name
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'Duplicate rows found in dataset_file(dataset_prefix, file_name). Resolve before applying migration.';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.naei_global_t_nfrcode
    WHERE nfr_code IS NOT NULL
    GROUP BY nfr_code
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'Duplicate rows found in naei_global_t_nfrcode(nfr_code). Resolve before applying migration.';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.naei_global_t_sourcename
    WHERE source_name IS NOT NULL
    GROUP BY source_name
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'Duplicate rows found in naei_global_t_sourcename(source_name). Resolve before applying migration.';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.naei_global_t_activityname
    WHERE activity_name IS NOT NULL
    GROUP BY activity_name
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'Duplicate rows found in naei_global_t_activityname(activity_name). Resolve before applying migration.';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM public.naei_global_t_pollutant
    WHERE pollutant IS NOT NULL
    GROUP BY pollutant
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'Duplicate rows found in naei_global_t_pollutant(pollutant). Resolve before applying migration.';
  END IF;
END
$$;

-- -----------------------------------------------------------------------------
-- 2) Add territory support to 2024 PV series.
-- -----------------------------------------------------------------------------
ALTER TABLE public.naei2024pv_series
  ADD COLUMN IF NOT EXISTS territory_name text;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.naei2024pv_series
    WHERE territory_name IS NULL OR btrim(territory_name) = ''
  ) THEN
    RAISE EXCEPTION 'naei2024pv_series.territory_name contains NULL/blank rows. Backfill territory values before continuing.';
  END IF;
END
$$;

ALTER TABLE public.naei2024pv_series
  ALTER COLUMN territory_name SET NOT NULL;

-- -----------------------------------------------------------------------------
-- 3) Add missing FK/unique constraints required for idempotent upserts.
-- -----------------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'dataset_file_dataset_prefix_file_name_key'
      AND conrelid = 'public.dataset_file'::regclass
  ) THEN
    ALTER TABLE public.dataset_file
      ADD CONSTRAINT dataset_file_dataset_prefix_file_name_key
      UNIQUE (dataset_prefix, file_name);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'naei2024pv_series_dataset_file_id_fkey'
      AND conrelid = 'public.naei2024pv_series'::regclass
  ) THEN
    ALTER TABLE public.naei2024pv_series
      ADD CONSTRAINT naei2024pv_series_dataset_file_id_fkey
      FOREIGN KEY (dataset_file_id) REFERENCES public.dataset_file(dataset_file_id);
  END IF;
END
$$;

-- Drop legacy 2023-style unique constraint copied onto 2024 table (no territory).
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'naei2023pv_series_dataset_file_id_pollutant_id_nfr_group_id_key'
      AND conrelid = 'public.naei2024pv_series'::regclass
  ) THEN
    ALTER TABLE public.naei2024pv_series
      DROP CONSTRAINT naei2023pv_series_dataset_file_id_pollutant_id_nfr_group_id_key;
  END IF;
END
$$;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.naei2024pv_series
    GROUP BY dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id, territory_name
    HAVING COUNT(*) > 1
  ) THEN
    RAISE EXCEPTION 'Duplicate rows found for naei2024pv_series series identity. Resolve before applying unique constraint.';
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'naei2024pv_series_identity_key'
      AND conrelid = 'public.naei2024pv_series'::regclass
  ) THEN
    ALTER TABLE public.naei2024pv_series
      ADD CONSTRAINT naei2024pv_series_identity_key
      UNIQUE (dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id, territory_name);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'naei2024pv_values_pv_series_id_fkey'
      AND conrelid = 'public.naei2024pv_values'::regclass
  ) THEN
    ALTER TABLE public.naei2024pv_values
      ADD CONSTRAINT naei2024pv_values_pv_series_id_fkey
      FOREIGN KEY (pv_series_id) REFERENCES public.naei2024pv_series(pv_series_id);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'naei_global_t_nfrcode_nfr_code_key'
      AND conrelid = 'public.naei_global_t_nfrcode'::regclass
  ) THEN
    ALTER TABLE public.naei_global_t_nfrcode
      ADD CONSTRAINT naei_global_t_nfrcode_nfr_code_key UNIQUE (nfr_code);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'naei_global_t_sourcename_source_name_key'
      AND conrelid = 'public.naei_global_t_sourcename'::regclass
  ) THEN
    ALTER TABLE public.naei_global_t_sourcename
      ADD CONSTRAINT naei_global_t_sourcename_source_name_key UNIQUE (source_name);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'naei_global_t_activityname_activity_name_key'
      AND conrelid = 'public.naei_global_t_activityname'::regclass
  ) THEN
    ALTER TABLE public.naei_global_t_activityname
      ADD CONSTRAINT naei_global_t_activityname_activity_name_key UNIQUE (activity_name);
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'naei_global_t_pollutant_pollutant_key'
      AND conrelid = 'public.naei_global_t_pollutant'::regclass
  ) THEN
    ALTER TABLE public.naei_global_t_pollutant
      ADD CONSTRAINT naei_global_t_pollutant_pollutant_key UNIQUE (pollutant);
  END IF;
END
$$;

COMMIT;
