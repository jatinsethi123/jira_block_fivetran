# From Abhay below:
view: __jk_jira_issue_fix_version {
  derived_table: {
    sql_trigger_value: SELECT CURRENT_DATE ;;
    sortkeys: ["issue_start_date"]
    distribution: "issue_id"
    sql: SELECT TRANSLATE(SUBSTRING(v.name,1,STRPOS(v.name,']')),'[]','  ') AS version_team
            ,  v.id AS issue_fix_version_id
            ,  v.name AS version_name
            ,  v.released AS is_version_released
            ,  v.start_date AS version_start_date
            ,  v.release_date AS version_release_date
            ,  v.description AS version_description
            ,  v.overdue AS is_version_overdue
            ,  i.start_date AS issue_start_date
            ,  i.key AS issue_key
            ,  i.id AS issue_id
            ,  i.priority AS issue_priority
            ,  i.assignee AS issue_assignee
            ,  r.name AS issue_resolution
            ,  NULLIF(r.name,'') IS NOT NULL AS has_issue_resolution
            ,  s.name AS issue_status
            ,  i.reporter AS issue_reporter
            ,  ist.name AS issue_type
            ,  i.resolved AS issue_resolved_datetime
            ,  i.created AS issue_created_datetime
            ,  i.description AS issue_description
            ,  i.summary AS issue_summary
            ,  i.story_points AS issue_story_points
            ,  NULLIF(i.story_points,'') IS NOT NULL AS has_story_points
            ,  e.key AS epic_key
            ,  e.name AS epic_name
            ,  engg_team.name AS engineering_team
          FROM jira.issue i
          LEFT JOIN jira.issue_fix_version_s ifv
            ON i.id = ifv.issue_id
          LEFT JOIN jira.version v
            ON v.id = ifv.version_id
          LEFT JOIN jira.resolution r
            ON r.id = i.resolution
          LEFT JOIN jira.status s
            ON s.id = i.status
          LEFT JOIN jira.issue_type ist
            ON ist.id = i.issue_type
          LEFT JOIN jira.epic e
            ON e.id = i.epic_link
          LEFT JOIN jira.field_option engg_team
            ON engg_team.id = i.engineering_team
         WHERE v.name like '[%]%' -- team names in [ ]s
         ;;
  }

  dimension: __jk_jira_issue_fix_version_id {
    hidden:yes
    primary_key: yes
    sql: ${TABLE}.issue_fix_version_id || ${TABLE}.issue_id ;;
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

  ## Epics
  dimension: epic_key {
    group_label: "Epic"
    type: string
  }

  dimension: epic_name {
    group_label: "Epic"
    type: string
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
