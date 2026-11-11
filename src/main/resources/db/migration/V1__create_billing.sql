create table subscription (
    iccid varchar(20) primary key,
    msisdn varchar(15) not null,
    status varchar(16) not null,
    last_event_at timestamptz not null
);

create table processed_event (
    event_id uuid primary key,
    outcome varchar(16) not null,
    processed_at timestamptz not null
);

create table charge (
    id uuid primary key,
    source_event_id uuid not null,
    iccid varchar(20) not null,
    msisdn varchar(15) not null,
    amount_minor bigint not null,
    currency varchar(3) not null,
    requested_at timestamptz not null
);

create unique index charge_source_event_idx on charge (source_event_id);

create table outbox (
    id uuid primary key,
    aggregate_id varchar(64) not null,
    topic varchar(128) not null,
    payload jsonb not null,
    created_at timestamptz not null,
    sent_at timestamptz
);

create index outbox_pending_idx on outbox (created_at, id) where sent_at is null;
