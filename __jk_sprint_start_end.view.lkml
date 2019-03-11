view: __jk_sprint_start_end {
  derived_table: {
    sql_trigger_value: SELECT CURRENT_DATE ;;
#    sortkeys: ["issue_start_date"]
    distribution: "issue_id"
    sql: with all_issues_and_sprints as (
select ish.issue_id,
         ish.sprint_id,
         CONVERT_TIMEZONE('UTC', 'America/New_York', s.start_date) AS nyc_issue_sprint_date
    from jira.issue_sprint ish
    left join jira.sprint s on s.id = ish.sprint_id
   where not exists ( SELECT 1 FROM jira.issue_sprint_history ishh
                       WHERE ishh.issue_id = ish.issue_id )
  UNION ALL
  select ish.issue_id,
         ish.sprint_id,
         CONVERT_TIMEZONE('UTC', 'America/New_York', ish.time) as nyc_issue_sprint_date
    from jira.issue_sprint_history ish
    left join jira.sprint s on s.id = ish.sprint_id
),
nyc_issue_sprint_dates_history as (
  select ish.issue_id,
         ish.sprint_id,
         ish.nyc_issue_sprint_date::date as nyc_issue_sprint_date,
         row_number() over (
              partition by ish.issue_id, ish.nyc_issue_sprint_date::date
              order by ish.nyc_issue_sprint_date DESC, ish.sprint_id DESC ) rnum
    from all_issues_and_sprints ish
    left join jira.sprint s on s.id = ish.sprint_id
),
nyc_issue_sprint_dates as (
  select isdh.issue_id,
         isdh.sprint_id,
         isdh.nyc_issue_sprint_date as effective_date,
         ISNULL( DATEADD(DAY,-1,
                     LEAD(isdh.nyc_issue_sprint_date) OVER (
                              PARTITION BY isdh.issue_id
                              ORDER BY isdh.nyc_issue_sprint_date)
                 )
           ,'9999-12-31') AS expiry_date
    from nyc_issue_sprint_dates_history isdh
   where isdh.rnum = 1
),
all_issues_and_points as (
  select i.id AS issue_id,
         i.story_points,
         CONVERT_TIMEZONE('UTC', 'America/New_York', i.created) as nyc_issue_points_date
    from jira.issue i
   where not exists ( SELECT 1 FROM jira.issue_story_points_history isph
                       WHERE isph.issue_id = i.id )
     and i.story_points is not null
  UNION ALL
  select isph.issue_id,
         isph.value AS story_points,
         CONVERT_TIMEZONE('UTC', 'America/New_York', isph.time) as nyc_issue_points_date
    from jira.issue_story_points_history isph
),
nyc_issue_points_dates_history as (
  select isph.issue_id,
         isph.story_points,
         isph.nyc_issue_points_date::date as nyc_issue_points_date,
         row_number() over (
              partition by isph.issue_id, isph.nyc_issue_points_date::date
              order by isph.nyc_issue_points_date DESC ) rnum
    from all_issues_and_points isph
),
nyc_issue_points_dates as (
  select ipdh.issue_id,
         ipdh.story_points,
         ipdh.nyc_issue_points_date as effective_date,
         ISNULL( DATEADD(DAY,-1,
                     LEAD(ipdh.nyc_issue_points_date) OVER (
                              PARTITION BY ipdh.issue_id
                              ORDER BY ipdh.nyc_issue_points_date)
                 )
           ,'9999-12-31') AS expiry_date
    from nyc_issue_points_dates_history ipdh
   where ipdh.rnum = 1
),
all_issues_and_statuses as (
  select i.id AS issue_id,
         i.status as status,
         CONVERT_TIMEZONE('UTC', 'America/New_York', i.created) as nyc_issue_status_date
    from jira.issue i
   where not exists ( SELECT 1 FROM jira.issue_status_history ish
                       WHERE ish.issue_id = i.id )
     --and i.status is not null
  UNION ALL
  select ish.issue_id,
         ish.status_id AS status,
         CONVERT_TIMEZONE('UTC', 'America/New_York', ish.time) as nyc_issue_status_date
    from jira.issue_status_history ish
),
nyc_issue_status_dates_history as (
  select ish.issue_id,
         ish.status,
         ish.nyc_issue_status_date::date as nyc_issue_status_date,
         row_number() over (
              partition by ish.issue_id, ish.nyc_issue_status_date::date
              order by ish.nyc_issue_status_date DESC ) rnum
    from all_issues_and_statuses ish
),
nyc_issue_status_dates as (
  select isdh.issue_id,
         isdh.status AS status_id,
         st.name AS status,
         isdh.nyc_issue_status_date as effective_date,
         ISNULL( DATEADD(DAY,-1,
                     LEAD(isdh.nyc_issue_status_date) OVER (
                              PARTITION BY isdh.issue_id
                              ORDER BY isdh.nyc_issue_status_date)
                 )
           ,'9999-12-31') AS expiry_date
    from nyc_issue_status_dates_history isdh
    left join jira.status st on st.id = isdh.status
   where isdh.rnum = 1
),
sprint_start_end_info as (
select ss.id AS sprint_id,
       isd_s.issue_id,
       ipd_s.story_points,
       ss.name AS sprint_name,
       CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date) AS sprint_start_date,
       CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date) AS sprint_complete_date ,
       istd_s.status as issue_status ,
       'sprint_start' date_type
  FROM jira.sprint ss
  LEFT JOIN nyc_issue_sprint_dates isd_s
    ON isd_s.sprint_id = ss.id
   AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date)::date between isd_s.effective_date and isd_s.expiry_date
  LEFT JOIN nyc_issue_points_dates ipd_s
    ON ipd_s.issue_id = isd_s.issue_id
   AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date)::date between ipd_s.effective_date and ipd_s.expiry_date
  LEFT JOIN nyc_issue_status_dates istd_s
    ON istd_s.issue_id = isd_s.issue_id
   AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date)::date between istd_s.effective_date and istd_s.expiry_date
  JOIN jira.issue i
    ON i.id = isd_s.issue_id
   AND i.issue_type != 5
UNION ALL
select ss.id AS sprint_id,
       isd_e.issue_id AS issue_id,
       ipd_e.story_points,
       ss.name AS sprint_name,
       CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date) AS sprint_start_date,
       CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date) AS sprint_complete_date ,
       istd_e.status as issue_status ,
       'sprint_end' date_type
  FROM jira.sprint ss
  LEFT JOIN nyc_issue_sprint_dates isd_e
    ON isd_e.sprint_id = ss.id
   AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date)::date between isd_e.effective_date and isd_e.expiry_date
  LEFT JOIN nyc_issue_points_dates ipd_e
    ON ipd_e.issue_id = isd_e.issue_id
   AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date)::date between ipd_e.effective_date and ipd_e.expiry_date
  LEFT JOIN nyc_issue_status_dates istd_e
    ON istd_e.issue_id = isd_e.issue_id
   AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date)::date between istd_e.effective_date and istd_e.expiry_date
  JOIN jira.issue i
    ON i.id = isd_e.issue_id
   AND i.issue_type != 5
)
/*
select s.sprint_name,
       b.name,
       b.type,
       s.sprint_start_date,
       s.sprint_complete_date,
       sp.complete_date,
       count(distinct CASE WHEN s.date_type = 'sprint_start' THEN s.issue_id ELSE NULL END) issue_count_start,
       sum(CASE WHEN s.date_type = 'sprint_start' THEN s.story_points ELSE NULL END) AS story_points_start,
       count(distinct CASE WHEN s.date_type = 'sprint_end' AND issue_status = 'Done' THEN s.issue_id ELSE NULL END) issue_count_end,
       sum(CASE WHEN s.date_type = 'sprint_end'  AND issue_status = 'Done' THEN s.story_points ELSE NULL END) AS story_points_end
  from sprint_start_end_info s
  join jira.sprint sp on sp.id = s.sprint_id
  left join jira.board b on b.id = sp.board_id
 where s.sprint_name = 'Stepreckon Sprint 26'
 group by 1, 2, 3, 4, 5, 6
;
*/
select i.key, s.*
  from sprint_start_end_info s
  left join jira.sprint sp on sp.id = s.sprint_id
  left join jira.board b on b.id = sp.board_id
  left join jira.issue i on i.id = s.issue_id
 where s.sprint_name = 'Stepreckon Sprint 26'
   and s.date_type = 'sprint_start'
 order by s.date_type desc, i.key
;;
      }

  ## Issue_Fix_Versions
  dimension: version_name {
    group_label: "Issue Fix Version"
    type:  string
  }

  dimension: is_version_released {
    group_label: "Issue Fix Version"
    type: yesno
  }

  dimension_group: version_release_date {
    group_label: "Version Release Date"
    type: time
    datatype: date
    timeframes: [date, month, year]
    convert_tz: no
  }

  dimension: version_description {
    group_label: "Issue Fix Version"
    type: string
  }

  dimension: is_version_overdue {
    group_label: "Issue Fix Version"
    type: yesno
  }

  dimension_group: version_start_date {
    group_label: "Version Start Date"
    type: time
    datatype: date
    timeframes: [date, month, year]
    convert_tz: no
  }

  ## Issues
  dimension: issue_key {
    group_label: "Issue"
    type: string
  }

  dimension_group: issue_start_date {
    group_label: "Issue Start Date"
    type: time
    datatype: date
    timeframes: [date, month, year]
    convert_tz: no
  }

  dimension: issue_priority {
    group_label: "Issue"
    type: number
  }

  dimension: issue_assignee {
    group_label: "Issue"
    type: string
  }

  dimension: issue_resolution {
    group_label: "Issue"
    type: string
  }

  dimension: has_issue_resolution {
    group_label: "Issue"
    type: yesno
  }

  dimension: issue_status {
    group_label: "Issue"
    type: string
  }

  dimension: issue_reporter {
    group_label: "Issue"
    type: string
  }

  dimension: issue_type {
    group_label: "Issue"
    type: string
  }

  dimension_group: issue_resolved_datetime {
    group_label: "Issue Resolved Date"
    type: time
    datatype: date
    timeframes: [date, month, year]
    convert_tz: no
  }

  dimension_group: issue_created_datetime {
    group_label: "Issue Created Date"
    type: time
    datatype: date
    timeframes: [date, month, year]
    convert_tz: no
  }

  dimension: issue_description {
    group_label: "Issue"
    type: string
  }

  dimension: issue_summary {
    group_label: "Issue"
    type: string
  }

  dimension: issue_story_points {
    group_label: "Issue"
    type: number
  }

  dimension: has_story_points {
    group_label: "Issue"
    type: yesno
  }

## Sprints
  dimension: sprint_name {
    group_label: "Sprint"
    type: string
  }

  dimension_group: sprint_start_date {
    group_label: "Sprint Start Date"
    type: time
    datatype: date
    timeframes: [date, month, year]
    convert_tz: no
  }

  dimension_group: sprint_end_date {
    group_label: "Sprint End Date"
    type: time
    datatype: date
    timeframes: [date, month, year]
    convert_tz: no
  }

  dimension_group: sprint_complete_date {
    group_label: "Sprint Complete Date"
    type: time
    datatype: date
    timeframes: [date, month, year]
    convert_tz: no
  }

  dimension: sprint_board_name {
    group_label: "Sprint"
    type: string
  }

  ## Epics
  dimension: epic_key {
    group_label: "Epic"
    type: string
  }

  dimension: epic_name {
    group_label: "Epic"
    type: string
  }

  dimension: is_epic_done {
    sql: ${TABLE}.epic_done ;;
    group_label: "Epic"
    type: yesno
  }

  ## Teams
  dimension: engineering_team {
    group_label: "Team"
    type: string
  }

  dimension: version_team {
    group_label: "Team"
    type: string
  }

  dimension: is_version_team {
    sql:${version_team} like '[%]%' ;;
    type: yesno
  }

  ## Measures ##

  measure: sum_of_story_points {
    group_label: "Totals"
    type:  sum
    sql: ${issue_story_points} ;;
  }

  measure: count_of_issues {
    group_label: "Totals"
    type:  count_distinct
    sql: ${issue_key} ;;
  }

}
