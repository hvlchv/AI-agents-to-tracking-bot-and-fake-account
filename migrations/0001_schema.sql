-- TODO: Add reviewed MVP schema before running Alembic.

CREATE TABLE app_user (
  id uuid PRIMARY KEY,
  username text NOT NULL UNIQUE,
  password_hash text NOT NULL,
  role text NOT NULL CHECK (role IN ('analyst','admin','viewer')),
  active boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE event_sample (
  event_id text PRIMARY KEY,
  source text NOT NULL CHECK (source IN ('rees46_multi','synthetic','demo_app')),
  dataset_version text NOT NULL,
  event_time timestamptz NOT NULL,
  user_id text NOT NULL,
  user_session text,
  event_type text NOT NULL CHECK (event_type IN ('view','cart','remove_from_cart','purchase','register','login','redeem')),
  product_id text, category_id text, category_code text, brand text,
  price numeric(18,4), currency text, device_id text, ip_token text, coupon_id text,
  attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
  ingested_at timestamptz NOT NULL DEFAULT now(),
  CHECK (jsonb_typeof(attributes) = 'object')
);
CREATE INDEX event_timeline_idx ON event_sample(source, user_id, event_time, event_id);

CREATE TABLE feature_window (
  id uuid PRIMARY KEY,
  source text NOT NULL,
  user_id text NOT NULL,
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  cutoff timestamptz NOT NULL,
  computed_at timestamptz NOT NULL DEFAULT now(),
  feature_version text NOT NULL,
  features jsonb NOT NULL,
  quality_flags jsonb NOT NULL DEFAULT '[]'::jsonb,
  UNIQUE(source,user_id,window_start,window_end,feature_version),
  CHECK (window_end = window_start + interval '5 minutes'),
  CHECK (cutoff = window_end)
);

CREATE TABLE risk_assessment (
  id uuid PRIMARY KEY,
  assessment_key text NOT NULL UNIQUE,
  feature_id uuid REFERENCES feature_window(id),
  source text NOT NULL,
  user_id text NOT NULL,
  target text NOT NULL CHECK (target IN ('bot','abuse')),
  window_start timestamptz NOT NULL,
  window_end timestamptz NOT NULL,
  scoring_method text NOT NULL,
  scoring_version text NOT NULL,
  policy_version text NOT NULL,
  provenance text NOT NULL CHECK (provenance IN ('fixture','rules','model')),
  raw_score double precision,
  risk_level text NOT NULL CHECK (risk_level IN ('LOW','MONITOR','REVIEW')),
  reason_codes jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (window_end = window_start + interval '5 minutes')
);

CREATE TABLE evidence (
  id uuid PRIMARY KEY,
  assessment_id uuid NOT NULL REFERENCES risk_assessment(id),
  reason_code text NOT NULL,
  evidence_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE casefile (
  id uuid PRIMARY KEY,
  case_key text NOT NULL UNIQUE,
  source text NOT NULL,
  subject_id text NOT NULL,
  target text NOT NULL CHECK (target IN ('bot','abuse')),
  title text NOT NULL,
  status text NOT NULL DEFAULT 'open' CHECK (status IN ('open','in_review','needs_info','closed')),
  assigned_to uuid REFERENCES app_user(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX case_list_idx ON casefile(status,created_at,id);

CREATE TABLE case_assessment (
  case_id uuid NOT NULL REFERENCES casefile(id),
  assessment_id uuid NOT NULL REFERENCES risk_assessment(id),
  PRIMARY KEY(case_id,assessment_id)
);

CREATE TABLE review (
  id uuid PRIMARY KEY,
  case_id uuid NOT NULL REFERENCES casefile(id),
  reviewer_id uuid NOT NULL REFERENCES app_user(id),
  conclusion text NOT NULL CHECK (conclusion IN ('legitimate','confirmed_bot','confirmed_abuse','inconclusive')),
  reason text NOT NULL CHECK (length(reason) BETWEEN 10 AND 2000),
  request_key text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE(reviewer_id,request_key)
);

CREATE TABLE audit_log (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  actor_id uuid REFERENCES app_user(id),
  action text NOT NULL,
  entity_type text NOT NULL,
  entity_id text NOT NULL,
  request_key text,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_record (
  scope text NOT NULL,
  request_key text NOT NULL,
  payload_hash text NOT NULL,
  response_status integer NOT NULL,
  response_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(scope,request_key)
);

CREATE TABLE redeem_transaction (
  id uuid PRIMARY KEY,
  account_id text NOT NULL,
  coupon_id text NOT NULL,
  status text NOT NULL CHECK (status IN ('accepted','rejected')),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX accepted_redeem_once_idx
  ON redeem_transaction(account_id,coupon_id) WHERE status='accepted';

CREATE TABLE outbox (
  id uuid PRIMARY KEY,
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  attempts integer NOT NULL DEFAULT 0
);
CREATE INDEX outbox_pending_idx ON outbox(created_at,id) WHERE published_at IS NULL;

CREATE TABLE action_proposal (
  id uuid PRIMARY KEY,
  case_id uuid NOT NULL REFERENCES casefile(id),
  proposed_by uuid NOT NULL REFERENCES app_user(id),
  action_type text NOT NULL CHECK (action_type IN ('monitor','request_more_info','suggest_block')),
  reason text NOT NULL,
  status text NOT NULL DEFAULT 'proposed' CHECK (status='proposed'),
  created_at timestamptz NOT NULL DEFAULT now()
);
