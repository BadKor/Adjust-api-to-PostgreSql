# Adjust-api-to-PostgreSql

Scripts for load data from Adjust to PostgreSql for Advertising Agency. API Doc https://help.adjust.com/en/article/kpi-service

### Table for data function "tracker" have delimeter by OS and Country:

create table table_name_for_trackers
(
    date          date,
    geo           text,
    app_name      text,
    app_token     text,
    tracker_name  text,
    tracker_token text,
    install       integer,
    pay_action    integer,
    os_names      text
);

# Table for data function "campaign" have delimeters by OS and Country:

create table table_name_for_campaigns
(
    date           date,
    geo            text,
    tracker_name   text,
    tracker_token  text,
    campaign_name  text,
    campaign_token text,
    install        integer,
    pay_action     integer,
    os_names       text
);

# Table for data function "sub_id" DON'T HAVE (It's Adjust rules) delimeters by OS and Country:

create table table_name_for_subids
(
    date           date,
    tracker_name   text,
    tracker_token  text,
    campaign_name  text,
    campaign_token text,
    sub_id         text,
    sub_id_token   text,
    install        integer,
    pay_action     integer
);

# Table for data function "creo" DON'T HAVE (It's Adjust rules) delimeters by OS and Country:

create table table_name_for_creatives
(
    date           date,
    tracker_name   text,
    tracker_token  text,
    campaign_name  text,
    campaign_token text,
    sub_id         text,
    creative_name  text,
    creative_token text,
    install        integer,
    pay_action     integer
);
