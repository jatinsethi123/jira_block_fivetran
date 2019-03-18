view: __jk_sprint_start_end {
  derived_table: {
    # run every two hours
    sql_trigger_value: SELECT FLOOR(EXTRACT(epoch from GETDATE()) / (2*60*60))  ;;
    sortkeys: ["sprint_start_date"]
    distribution: "issue_key"
    sql:  with clean_issue_sprint_history as (
            -- to handle duplicate timestamps down to last millisecond !
            select ish.issue_id,
                   CONVERT_TIMEZONE('UTC', 'America/New_York', ish.time) as nyc_issue_sprint_date,
                   MAX(ish.sprint_id) AS sprint_id
              from jira.issue_sprint_history ish
             group by 1, 2
          ),
          all_issues_and_sprints_history as (
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
                   ish.nyc_issue_sprint_date
              from clean_issue_sprint_history ish
          ),
          nyc_issue_sprint_dates as (
            select isdh.issue_id,
                   isdh.sprint_id,
                   isdh.nyc_issue_sprint_date as effective_date,
                   ISNULL( DATEADD(MILLISECOND,-1,
                               LEAD(isdh.nyc_issue_sprint_date) OVER (
                                        PARTITION BY isdh.issue_id
                                        ORDER BY isdh.nyc_issue_sprint_date)
                           )
                     ,'9999-12-31') AS expiry_date
              from all_issues_and_sprints_history isdh
          ),
          clean_issue_points_history as (
            -- to handle duplicate timestamps down to last millisecond !
            select isph.issue_id,
                   CONVERT_TIMEZONE('UTC', 'America/New_York', isph.time) as nyc_issue_points_date,
                   MAX(isph.value) AS story_points
              from jira.issue_story_points_history isph
             group by 1, 2
          ),
          all_issue_points_history as (
            select i.id AS issue_id,
                   i.story_points,
                   CONVERT_TIMEZONE('UTC', 'America/New_York', i.created) as nyc_issue_points_date
              from jira.issue i
             where not exists ( SELECT 1 FROM jira.issue_story_points_history isph
                                 WHERE isph.issue_id = i.id )
               and i.story_points is not null
            UNION ALL
            select isph.issue_id,
                   isph.story_points,
                   isph.nyc_issue_points_date
              from clean_issue_points_history isph
          ),
          nyc_issue_points_dates as (
            select ipdh.issue_id,
                   ipdh.story_points,
                   ipdh.nyc_issue_points_date as effective_date,
                   ISNULL( DATEADD(MILLISECOND,-1,
                               LEAD(ipdh.nyc_issue_points_date) OVER (
                                        PARTITION BY ipdh.issue_id
                                        ORDER BY ipdh.nyc_issue_points_date)
                           )
                     ,'9999-12-31') AS expiry_date
              from all_issue_points_history ipdh
          ),
          all_issue_status_history as (
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
          nyc_issue_status_dates as (
            select isdh.issue_id,
                   isdh.status AS status_id,
                   st.name AS status,
                   isdh.nyc_issue_status_date as effective_date,
                   ISNULL( DATEADD(MILLISECOND,-1,
                               LEAD(isdh.nyc_issue_status_date) OVER (
                                        PARTITION BY isdh.issue_id
                                        ORDER BY isdh.nyc_issue_status_date)
                           )
                     ,'9999-12-31') AS expiry_date
              from all_issue_status_history isdh
              left join jira.status st on st.id = isdh.status
          ),
          sprint_start_end_info as (
            select ss.id AS sprint_id,
                   isd_s.issue_id,
                   ipd_s.story_points,
                   ss.name AS sprint_name,
                   isd_s.effective_date AS date_added_to_sprint,
                   CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date) AS sprint_start_date,
                   CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date) AS sprint_complete_date ,
                   istd_s.status as issue_status ,
                   'sprint_start' date_type
              FROM jira.sprint ss
              LEFT JOIN nyc_issue_sprint_dates isd_s
                ON isd_s.sprint_id = ss.id
               AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date) between isd_s.effective_date and isd_s.expiry_date
              LEFT JOIN nyc_issue_points_dates ipd_s
                ON ipd_s.issue_id = isd_s.issue_id
               AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date) between ipd_s.effective_date and ipd_s.expiry_date
              LEFT JOIN nyc_issue_status_dates istd_s
                ON istd_s.issue_id = isd_s.issue_id
               AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date) between istd_s.effective_date and istd_s.expiry_date
              JOIN jira.issue i
                ON i.id = isd_s.issue_id
               AND i.issue_type != 5
            UNION ALL
            select ss.id AS sprint_id,
                   isd_e.issue_id AS issue_id,
                   ipd_e.story_points,
                   ss.name AS sprint_name,
                   isd_e.effective_date AS date_added_to_sprint,
                   CONVERT_TIMEZONE('UTC', 'America/New_York', ss.start_date) AS sprint_start_date,
                   CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date) AS sprint_complete_date ,
                   istd_e.status as issue_status ,
                   'sprint_end' date_type
              FROM jira.sprint ss
              LEFT JOIN nyc_issue_sprint_dates isd_e
                ON isd_e.sprint_id = ss.id
               AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date) between isd_e.effective_date and isd_e.expiry_date
              LEFT JOIN nyc_issue_points_dates ipd_e
                ON ipd_e.issue_id = isd_e.issue_id
               AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date) between ipd_e.effective_date and ipd_e.expiry_date
              LEFT JOIN nyc_issue_status_dates istd_e
                ON istd_e.issue_id = isd_e.issue_id
               AND CONVERT_TIMEZONE('UTC', 'America/New_York', ss.complete_date) between istd_e.effective_date and istd_e.expiry_date
              JOIN jira.issue i
                ON i.id = isd_e.issue_id
               AND i.issue_type != 5
          )
          select i.key AS issue_key,
                 s.sprint_id,
                 s.issue_id,
                 s.story_points,
                 s.sprint_name,
                 s.date_added_to_sprint,
                 s.sprint_start_date,
                 sp.end_date AS sprint_end_date,
                 s.sprint_complete_date,
                 s.issue_status,
                 s.date_type AS sprint_start_end_type,
                 b.name AS board_name,
                 ist.name AS issue_type,
                 i.start_date AS issue_start_date
            from sprint_start_end_info s
            left join jira.sprint sp on sp.id = s.sprint_id
            left join jira.board b on b.id = sp.board_id
            left join jira.issue i on i.id = s.issue_id
            left join jira.issue_type ist on ist.id = i.issue_type
      ;;
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

  dimension: date_added_to_sprint  {
    type: date_time
    description: "DateTime Issue Added to Sprint"
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

  dimension: issue_story_points {
    group_label: "Issue"
    type: number
    sql: ${TABLE}.story_points ;;
  }

## Sprints
  dimension: sprint_start_end_type {
    group_label: "Sprint"
    type: string
  }

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

  dimension: board_name {
    group_label: "Sprint"
    type: string
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
