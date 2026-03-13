# fabric-monitoring
Monitoring utilities for Microsoft Fabric workspaces

> This is an independent community project and is not an official Microsoft offering.

## Real-Time Dashboards
Links to `dashboard.json` template files:

- [Single Workspace](alpha/Workspace%20Monitoring%20Dashboard.KQLDashboard/RealTimeDashboard.json)
- [Multiple Workspaces](beta/Multi-Workspace%20Hub.KQLDashboard/RealTimeDashboard.json)

#### Import instructions:
1. Edit dashboard.
2. Upload `.json` template file.
3. Point to KQL Database source.

###

![alt text](image.png)

###

## KQL Starters

#### Aggreagte logs across multiple workspaces
```kusto
.create-or-alter function with (
	folder = "VirtualTables",
	docstring = "Unified monitoring item job event logs from all workspace monitoring databases",
	skipvalidation = "true",
	view = true
) AllItemJobEventLogs() {
	union isfuzzy=true withsource=SourceWorkspace
		(
			cluster('**INSERT QUERY_URI_1**').database('Monitoring Eventhouse').ItemJobEventLogs
			| where Timestamp > ago(30d)
			| extend Workspace = "**INSERT WORKSPACE_NAME_1**"
		),
		(
			cluster('**INSERT QUERY_URI_2**').database('Monitoring Eventhouse').ItemJobEventLogs
			| where Timestamp > ago(30d)
			| extend Workspace = "**INSERT WORKSPACE_NAME_2**"
		)
		// Add as many workspace blocks as needed by repeating the pattern below:
		// ,(
		//     cluster('**INSERT QUERY_URI_N**').database('Monitoring Eventhouse').ItemJobEventLogs
		//     | where Timestamp > ago(30d)
		//     | extend Workspace = "**INSERT WORKSPACE_NAME_N**"
		// )
}
```

#### Multi-workspace failed pipeline queryset

```kusto
AllItemJobEventLogs
| extend SecondsAgo = datetime_diff('second', now(), ingestion_time())
| where JobType == 'Pipeline' and JobStatus == 'Failed' and SecondsAgo <= 480
| order by Timestamp desc
| project Timestamp, SecondsAgo, ItemName, WorkspaceName, JobScheduleTime, JobStartTime, JobEndTime, JobStatus
```

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).