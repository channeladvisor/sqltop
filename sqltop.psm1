function Invoke-SqlTop {
    <#
    .SYNOPSIS
    Shows the top resource consuming sessions on an instance.

    .DESCRIPTION


    .PARAMETER SqlInstance
    SQL Server instance you want to connect to.

    .PARAMETER SqlAuth
    Use SQL Server auth, if a sqltop.cred file exists in the users home directory those credentials will be used, otherwise the user will be asked to provide auth

    .PARAMETER Credential
    If this is supplied SqlAuth is implied, and the passed credential will be used

    .NOTES
        TODO: Move the render code into the data refresh scriptblock to avoid screen flicker, goal ~5-10ms re-draw time. Might have to Change how the header is drawn as well.

    #>

    [CmdletBinding()]
    param (
        [Parameter(Mandatory=$True)]
        [string]$SqlInstance,
        [switch]$SqlAuth = $False,
        [pscredential]$Credential
    )

    # Set up a synchronized hashtable to pass values between the UI and Data layers
    $StateData = [hashtable]::Synchronized(@{})
    
    # Add more properties to the StateData hashtable
    if ( $SqlAuth -or $Credential ) {
        $StateData.SqlAuth = $True
    } else {
        $StateData.SqlAuth = $False
    }
    
    $OldTitle = $Host.UI.RawUI.WindowTitle
    $Host.UI.RawUI.WindowTitle = "SQLTop"

    $StateData.Debug = $False
    $StateData.SqlInstance = $SqlInstance
    $StateData.Updated = Get-Date
    $StateData.DataRefreshSec = 5
    $StateData.DataRefreshDefaultSec = 5
    $StateData.DisplayMode ='Resource Usage'
    $StateData.Run = $True
    $StateData.Reset = $False

    if ( $SqlAuth ) {
        try {
            if ( Test-Path -Path "${env:userprofile}\sqltop.cred" -ErrorAction SilentlyContinue ) {
                $Credential = Import-Clixml -Path "${env:\userprofile}\sqltop.cred"
            } elseif ( Test-Path -Path "${env:HOME}\sqltop.cred" -ErrorAction SilentlyContinue ) {
                $Credential = Import-Clixml -Path "${env:HOME}\sqltop.cred"
            } else {
                $Credential = Get-Credential -Message "No credential file found at '${env:\userprofile}\sqltop.cred', please enter your credentials" -Title "SQLTop Credentials - $($SqlInstance)"
            }
        } catch {
            Write-Host "Error processing credentials file: $($_.Exception.Message)"
            return
        }
    }

    if ( $Credential ) {
        $StateData.UserName = $Credential.UserName
        $StateData.Password = $Credential.GetNetworkCredential().password
        $StateData.SqlAuth = $True

        Write-Verbse "    User: $($StateData.UserName)"
        Write-Verbse "Password: $($StateData.Password)"
    }

    # Set some starting variables
    $UI_refresh_sec = 5

    function color {
        param(
            $Text,
            $ForegroundColor = 'default',
            $BackgroundColor = 'default'
        )
        # Terminal Colors
        $Colors = @{
            "default" = @(40,50)
            "black" = @(30,0)
            "lightgrey" = @(33,43)
            "grey" = @(37,47)
            "darkgrey" = @(90,100)
            "red" = @(91,101)
            "darkred" = @(31,41)
            "green" = @(92,102)
            "darkgreen" = @(32,42)
            "yellow" = @(93,103)
            "white" = @(97,107)
            "brightblue" = @(94,104)
            "darkblue" = @(34,44)
            "indigo" = @(35,45)
            "cyan" = @(96,106)
            "darkcyan" = @(36,46)
        }
    
        if ( $ForegroundColor -notin $Colors.Keys -or $BackgroundColor -notin $Colors.Keys) {
            Write-Error "Invalid color choice!" -ErrorAction Stop
        }
    
        "$([char]27)[$($colors[$ForegroundColor][0])m$([char]27)[$($colors[$BackgroundColor][1])m$($Text)$([char]27)[0m"    
    }

    # Display and sort options per display mode
    $DisplayColumns = @{
        'Object Tracking' = (
            'object',
            'workers',
            'blocked',
            'cpu',
            @{
                Name = 'mem_mb'
                Expression = { "{0:0.00}" -f $_.mem_mb }
                Alignment = "right"
            },
            @{
                Name = 'tempdb_mb'
                Expression = { "{0:0.00}" -f $_.tempdb_mb }
                Alignment = "right"
            },
            @{
                Name = 'lread_mb'
                Expression = { "{0:0.00}" -f $_.lread_mb }
                Alignment = "right"
            }
        )
        'Waits' = (
            'x',
            'spid',
            @{
                Name='duration'
                Expression={([timespan]::fromseconds($_.dur_sec)).ToString('d\.hh\:mm\:ss')}
                Alignment = "right"
            },
            'block',
            'status',
            'user',
            'database',
            'program_name',
            'command',
            'wt_ms',
            'wt_type',
            'wt_rsrc',
            'open_tran'
        )
        'Waits_spid_track' = (
            'x',
            'spid',
            @{
                Name='duration'
                Expression={([timespan]::fromseconds($_.dur_sec)).ToString('d\.hh\:mm\:ss')}
                Alignment = "right"
            },
            'block',
            'status',
            'user',
            'database',
            'program_name',
            'command',
            'wt_ms',
            'wt_type',
            'wt_rsrc',
            'open_tran',
            @{
                Name = '|'
                Expression = {'|'}
            },
            @{
                Name = "wt_ms $([char](8710))"
                Expression = {
                    $CurrSpid = $_.spid
                    $CurrEcid = $_.ecid
                    $CurrWtType = $_.wt_type
                    $CurrWtMs = $_.wt_ms
                    $StateData.PrevResults | Where-Object { $_.spid -eq $CurrSpid -and $_.ecid -eq $CurrEcid -and $_.wt_type -eq $CurrWtType } | ForEach-Object {
                        $Delta = $($CurrWtMs - $_.wt_ms)
                        $Delta
                    }
                }
                Alignment = "right"
            }
        )
        'Waits_summary' = (
            'wait_type',
            'spid_count',
            'total_wait_ms'
        )
        'Resource Usage' = (
            'x',
            'spid',
            @{
                Name='duration'
                Expression={([timespan]::fromseconds($_.dur_sec)).ToString('d\.hh\:mm\:ss')}
                Alignment = "right"
            },
            'block',
            'status',
            'user',
            'hostname',
            'database',
            'program_name',
            'command',
            'host_pid',
            @{
                Name = 'mem_mb'
                Expression = { "{0:0.00}" -f $_.mem_mb }
                Alignment = "right"
            },
            'ss',
            @{
                Name = 'tempdb_mb'
                Expression = { "{0:0.00}" -f $_.tempdb_mb }
                Alignment = "right"
            },
            @{
                Name = 'lread_mb'
                Expression = { "{0:0.00}" -f $_.lread_mb }
                Alignment = "right"
            },
            'cpu',
            'open_tran'
        )
        'Blocking' = (
            'x',
            'spid',
            'block',
            @{
                Name='duration'
                Expression={([timespan]::fromseconds($_.dur_sec)).ToString('d\.hh\:mm\:ss')}
                Alignment = "right"
            },
            'status',
            'user',
            'hostname',
            'database',
            'program_name',
            'command',
            'host_pid',
            'wt_ms',
            'wt_type',
            'wt_rsrc',
            'open_tran'
        )
        'SpidHistory' = (
            @{
                Name = 'ctime_utc'
                Expression = { $_.collection_time_utc }
            },
            'x',
            'spid',
            @{
                Name='duration'
                Expression={([timespan]::fromseconds($_.dur_sec)).ToString('d\.hh\:mm\:ss')}
                Alignment = "right"
            },
            'block',
            'status',
            'user',
            'hostname',
            'database',
            'program_name',
            'command',
            'host_pid',
            @{
                Name = 'mem_mb'
                Expression = { "{0:0.00}" -f $_.mem_mb }
                Alignment = "right"
            },
            @{
                Name = 'tempdb_mb'
                Expression = { "{0:0.00}" -f $_.tempdb_mb }
                Alignment = "right"
            },
            @{
                Name = 'lread_mb'
                Expression = { "{0:0.00}" -f $_.lread_mb }
                Alignment = "right"
            },
            'cpu',
            'open_tran',
            'wt_ms',
            'wt_type',
            'wt_rsrc'
        )
        'Resources by Program' = (
            'program_name',
            'spids',
            'workers',
            'blocked',
            'total_cpu',
            @{
                Name = "total_l_reads_mb"
                Expression = { "{0:0.00}" -f $_.total_l_reads_mb }
                Alignment = "right"
            },
            @{
                Name = "total_tempdb_mb"
                Expression = { "{0:0.00}" -f $_.total_tempdb_mb }
                Alignment = "right"
            }
        )
        'Resource Usage_spid_track' = (
            'x',
            'spid',
            @{
                Name='duration'
                Expression={([timespan]::fromseconds($_.dur_sec)).ToString('d\.hh\:mm\:ss')}
            },
            'block',
            'status',
            'user',
            'hostname',
            'database',
            'program_name',
            'command',
            'host_pid',
            'open_tran',
            @{
                Name = 'mem_mb'
                Expression = { "{0:0.00}" -f $_.mem_mb }
                Alignment = "right"
            },
            @{
                Name = 'tempdb_mb'
                Expression = { "{0:0.00}" -f $_.tempdb_mb }
                Alignment = "right"
            },
            @{
                Name = 'lread_mb'
                Expression = { "{0:0.00}" -f $_.lread_mb }
                Alignment = "right"
            },
            'cpu',
            @{
                Name = '|'
                Expression = {'|'}
            },
            @{
                Name = "tempdb_mb $([char](8710))"
                Expression = {
                    $CurrSpid = $_.spid
                    $CurrEcid = $_.ecid
                    $CurrTempdb = $_.tempdb_mb
                    $StateData.PrevResults | Where-Object { $_.spid -eq $CurrSpid -and $_.ecid -eq $CurrEcid } | ForEach-Object {
                        $Delta = $($CurrTempdb - $_.tempdb_mb)
                        "{0:0.00}" -f $Delta
                    }
                }
                Alignment = "right"
            },
            @{
                Name = "lread_mb $([char](8710))"
                Expression = {
                    $CurrSpid = $_.spid
                    $CurrEcid = $_.ecid
                    $CurrReads = $_.lread_mb
                    $StateData.PrevResults | Where-Object { $_.spid -eq $CurrSpid -and $_.ecid -eq $CurrEcid } | ForEach-Object {
                        $Delta = $($CurrReads - $_.lread_mb)
                        "{0:0.00}" -f $Delta
                    }
                }
                Alignment = "right"
            },
            @{
                Name="cpu $([char](8710))"
                Expression = {
                    $CurrSpid = $_.spid
                    $CurrEcid = $_.ecid
                    $CurrCPU = $_.cpu
                    $StateData.PrevResults | Where-Object { $_.spid -eq $CurrSpid -and $_.ecid -eq $CurrEcid } | ForEach-Object {
                        $Delta = $($CurrCPU - $_.cpu)
                        $Delta
                    }
                }
                Alignment = "right"
            }
        )
    }

    $SortOptions = @{
        'Object Tracking' = (
            @{
                Expression = { if ("$($_.object)" -eq 'No associated proc'){2} else {1} }
                Descending = $False
            },
            @{
                Expression = 'workers'
                Descending = $True
            }
        )
        'Waits' = (
            @{
                Expression = {if($filter -and $($_ | Select-Object * | Out-String) -match $filter){1} else {2}}
                Descending = $False
            },
            @{
                Expression = 'group_wait'
                Descending = $True 
            },
            @{
                Expression = 'ecid'
                Descending = $False
            }
        )
        'Waits_summary' = (
            @{
                Expression = 'total_wait_ms'
                Descending = $True
            }
        )
        'Resource Usage' = (
            @{
                Expression = {if($filter -and $($_ | Select-Object * | Out-String) -match $filter){1} else {2}}
                Descending = $False
            },
            @{
                Expression = 'group_status'
                Descending = $False
            },
            @{
                Expression = 'spid'
                Descending = $False
            },
            @{
                Expression = 'ecid'
                Descending = $False
            }
        )
        'SpidHistory' = (
            @{
                Expression = 'collection_time_utc'
                Descending = $True
            },
            @{
                Expression = 'group_status'
                Descending = $False
            },
            @{
                Expression = 'spid'
                Descending = $False
            },
            @{
                Expression = 'ecid'
                Descending = $False
            }
        )
        'Blocking' = @{
            Expression = {"$(($_.blockingchain)-join(''))-$($_.spid)"}
            Descending = $False
        }
        'Resources by Program' = (
            @{
                Expression = 'total_cpu'
                Descending = $True
            }
        )
    }

    # Define the script that will be run in the data collection process
    $DataRefreshCmd = [PowerShell]::Create().AddScript({
    $Query = @"
    SET QUOTED_IDENTIFIER ON
    SET ANSI_NULL_DFLT_ON ON
    SET ANSI_PADDING ON
    SET ANSI_NULLS ON
    SET CONCAT_NULL_YIELDS_NULL ON
    SET ARITHABORT ON
    
    SELECT
        sp.spid AS spid,
        sp.ecid AS ecid,
        sp.blocked AS block,
        ISNULL(DATEDIFF(second,er.start_time,GETDATE()),0) AS dur_sec,
        CASE sp.status
        WHEN 'pending' THEN 0
        WHEN 'running' THEN 1
        WHEN 'runnable' THEN 2
        WHEN 'spinloop' THEN 3
        WHEN 'rollback' THEN 4
        WHEN 'suspended' THEN 5
        WHEN 'background' THEN 6
        ELSE 7
        END as status_id,
        RTRIM(sp.status) AS status,
        RTRIM(sp.hostname) AS hostname,
        DB_NAME(sp.dbid) AS [database],
        CAST(sp.hostprocess AS BIGINT) AS host_pid,
        sp.waittime AS wt_ms,
        SUM(sp.waittime) OVER( PARTITION BY spid ORDER BY (SELECT NULL) ) AS group_wait,
        RTRIM(sp.lastwaittype) As wt_type,
        RTRIM(sp.waitresource) as wt_rsrc,
        sp.cpu,
        sp.memusage / 128.0 AS mem_mb,
        sp.physical_io AS pread_mb,
        ISNULL(su.tempdb_mb,0) AS tempdb_mb,
        ISNULL(er.logical_reads,0)/128.0 AS lread_mb,
        ISNULL(snapshot_transaction.is_snapshot,0) AS ss,
        sp.open_tran,
        NULLIF(RTRIM(sp.nt_domain) + '\' + RTRIM(sp.nt_username),'\') AS [user],
        RTRIM(sp.program_name) AS program_name,
        RTRIM(sp.cmd) AS command,
        er.plan_handle,
        er.statement_sql_handle,
        ISNULL(object_schema_name(ps.object_id,ps.database_id) + '.' + object_name(ps.object_id,ps.database_id),'No associated proc') AS [proc_name],
        getutcdate() AS collection_time_utc
    FROM	sys.sysprocesses AS sp WITH(NOLOCK)
            OUTER APPLY ( 
                SELECT SUM(su.user_objects_alloc_page_count + su.internal_objects_alloc_page_count) / 128.0 AS tempdb_mb
                FROM sys.dm_db_task_space_usage AS su WITH(NOLOCK)
                WHERE sp.spid = su.session_id
            ) AS su
            LEFT OUTER JOIN sys.dm_exec_requests AS er WITH(NOLOCK)
                ON sp.spid = er.session_id
                AND er.scheduler_id IN ( SELECT scheduler_id FROM sys.dm_os_schedulers WHERE status = 'VISIBLE ONLINE' )
            OUTER APPLY (
                SELECT CAST(is_snapshot AS INT) AS is_snapshot FROM sys.dm_tran_active_snapshot_database_transactions AS sst WHERE sst.session_id = sp.spid
            ) AS snapshot_transaction
            LEFT OUTER JOIN sys.dm_exec_procedure_stats AS ps WITH(NOLOCK)
                ON er.plan_handle = ps.plan_handle
    WHERE	sp.spid <> @@SPID
            AND (
                NOT ( sp.lastwaittype = 'MISCELLANEOUS' AND sp.open_tran = 0 )
                OR su.tempdb_mb > 0
            )
"@
        
        [string]$GetExecPlan_Query = @'
        SELECT p.query_plan 
        FROM sys.dm_exec_requests AS r
        OUTER APPLY sys.dm_exec_text_query_plan(
                        r.plan_handle,
                        r.statement_start_offset,
                        r.statement_end_offset) AS p
        WHERE r.session_id = {{spid}}
'@

        while($StateData.Run) {
            try {
                if ( $StateData.Reset ) {
                    $StateData.Error = $null
                    $StateData.InputBuffer = $null
                    $StateData.SpidFilter = $null
                    $StateData.Results = $null
                    $StateData.Lock = $False
                    $StateData.Reset = $False
                }

                while ( $StateData.Lock ) { Start-Sleep -Milliseconds 10 }

                $WhileStart = Get-Date
                if ( -not $IsLinux ) {
                    try {
                        $StateData.cpu = [int]((Get-Counter -ComputerName "$($StateData.SqlInstance)" -Counter '\Processor(_Total)\% User Time').CounterSamples | Select-Object -ExpandProperty CookedValue)
                    } catch {
                        $StateData.cpu = 0
                    }
                }

                # Get, and time, our data
                $StateData.QueryStart = Get-Date

                if ( $StateData.SqlAuth ) {
                    $ConnectionString = "Data Source=$($StateData.SqlInstance);Initial Catalog=master;User Id=$($StateData.UserName);Password=$($StateData.Password);Application Name=SQLTop;"
                } else {
                    $ConnectionString = "Data Source=$($StateData.SqlInstance);Initial Catalog=master;Integrated Security=True;Application Name=SQLTop;"
                }

                $Conn = [System.Data.Sqlclient.SqlConnection]::new()
                $Conn.ConnectionString = $ConnectionString
                $Conn.Open()
                $Cmd = [System.Data.Sqlclient.SqlCommand]::new()
                
                $Cmd.CommandText = $Query
                $Cmd.CommandTimeout = 108000
                $Cmd.Connection = $Conn
                $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
                $SqlAdapter.SelectCommand = $Cmd
                $DataSet = New-Object System.Data.DataSet
                $null = $SqlAdapter.Fill($DataSet)

                $Results = $DataSet.Tables[0].Rows

                $QueryStop = Get-Date
                $StateData.QueryTiming = $(($QueryStop - $StateData.QueryStart).TotalMilliseconds)

                # Determine if we need to back off
                $QueryRatio = $($StateData.QueryTiming / ($StateData.DataRefreshSec * 1000))
                if ( $QueryRatio -gt 2 ) {
                    $StateData.DataRefreshSec = $StateData.DataRefreshSec + 5
                } elseif ( $QueryRatio -lt 1 -and $StateData.DataRefreshSec -gt 5 ) {
                    $StateData.DataRefreshSec = $StateData.DataRefreshSec - 5
                }

                $Blockers = $Results | Select-Object -ExpandProperty block -Unique
                # Capture parallel spids, this is used later to make sure we display ALL workers for a given spid
                $ParallelSpids = $Results | Where-Object { $_.ecid -and $_.ecid -gt 0 } | Select-Object -ExpandProperty spid -Unique
    
                $Results | Add-Member -MemberType NoteProperty -Name 'group_status' -Value -1 -Force
                $Results | Add-Member -MemberType NoteProperty -Name 'x' -Value [string]'' -Force
    
                for ($i = 0; $i -lt $Results.Count; $i++) {
                    if ($Results[$i].ecid -gt 0) {
                        $Results[$i].x = ' --> '
                        $Results[$i].group_status = [int]($($Results | Where-Object { $_.spid -eq $Results[$i].spid -and $_.ecid -eq 0 } | Select-Object -ExpandProperty status_id -First 1))
                    } else {
                        $Results[$i].x = '-'
                        $Results[$i].group_status = $Results[$i].status_id
                    }
    
                    if ( $Results[$i].program_name.length -gt 53 -and $StateData.DisplayMode -ne 'Resources by Program' ) {
                        $Results[$i].program_name = "$($Results[$i].program_name.SubString(0,50))..."
                    }
                }
    
                if ( $StateData.DisplayMode -eq 'Blocking' ) {
                    $Results | Add-Member -MemberType NoteProperty -Name 'blockingchain' -Value @() -Force
                    $BlockingChains = $Results | Where-Object { $_.block -ne 0 -or $_.spid -in $Blockers}
                    
                    $BlockingChains | ForEach-Object {
                        #if ( $_.block -eq 0 ) {
                        #    $_.blockingchain += $_.spid
                        #}
    
                        $_.blockingchain += $_.block
                        $block = $_.block
                        while($block) {
                            $block = $BlockingChains | Where-Object { $_.spid -eq $block } | Select-Object -ExpandProperty block
                            $_.blockingchain += $block
                        }
                    
                        #$_.blockingchain = $_.blockingchain | Where-Object { $_ -ne 0 }
                
                        if ( $_.block -eq 0 ) {
                            $_.x = ' |'
                        } else {
                            $_.x = '+-'.PadLeft($($_.blockingchain.count+2),' ')
                        }
                        [array]::Reverse($_.blockingchain)
                    }
    
                    $StateData.PrevResults = $StateData.Results
                    $StateData.Results = $BlockingChains
                    $StateData.Error = $null
                    $StateData.Updated = Get-Date
                    $StateData.HasNewData = $True
                } else {
                    $StateData.PrevResults = $StateData.Results
                    $StateData.Results = $Results | Where-Object {
                        $_.open_tran -gt 0 `
                        -or ( 
                            $_.status -notin ('background','sleeping','suspended','dormant')
                        ) -or (
                            $_.spid -in $Blockers
                        ) -or (
                            $_.spid -in $ParallelSpids
                        ) -or (
                            $_.tempdb_mb -gt 0
                        )
                    }
                    $StateData.Error = $null
                    $StateData.Updated = Get-Date
                    $StateData.HasNewData = $True
                }

                if ( $StateData.SpidFilter ) {
                    try {
                        $Cmd.CommandText = "DBCC INPUTBUFFER($($StateData.SpidFilter));"
                        $Cmd.CommandTimeout = 108000
                        $Cmd.Connection = $Conn
                        $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
                        $SqlAdapter.SelectCommand = $Cmd
                        $DataSet = New-Object System.Data.DataSet
                        $null = $SqlAdapter.Fill($DataSet)
                        $StateData.InputBuffer = $DataSet.Tables[0].Rows | Select-Object -ExpandProperty EventInfo
                    } catch {
                        $StateData.InputBuffer = "No statement found"
                    }
                }

                if ( $StateData.GetPlan ) {
                    try {
                        $RootPath = "$($env:HOMEDRIVE)$($env:HOMEPATH)\SQLTop\"
                        New-Item -ItemType Directory -Path $RootPath -Force
                        $PlanPath ="$(Join-Path -Path $RootPath -ChildPath "$(Get-Date -Format "yyyMMdd_HHmmss")_SPID$($StateData.SpidFilter)-$(New-GUId).sqlplan")"
                        $FinalQuery = "$($GetExecPlan_Query.Replace('{{spid}}',$($StateData.SpidFilter)))"

                        $Cmd.CommandText = $FinalQuery
                        $Cmd.CommandTimeout = 108000
                        $Cmd.Connection = $Conn
                        $SqlAdapter = New-Object System.Data.SqlClient.SqlDataAdapter
                        $SqlAdapter.SelectCommand = $Cmd
                        $DataSet = New-Object System.Data.DataSet
                        $null = $SqlAdapter.Fill($DataSet)

                        $PlanData = $DataSet.Tables[0].Rows | Select-Object -ExpandProperty query_plan
                        $PlanData | Out-File -Encoding utf8 -Force -FilePath $PlanPath
                        
                        $StateData.PlanMessage = "Plan available @ $($PlanPath)$($Message)"
                    } catch {
                        $StateData.PlanMessage = "Unable to get plan: $($_.Exception.Message)"
                    }

                    $StateData.GetPlan = $False
                }

                $WhileStop = Get-Date
                $StateData.WhileTiming = $(($WhileStop - $WhileStart).TotalMilliseconds)
                while ( ((Get-Date) - $StateData.Updated).TotalSeconds -le $StateData.DataRefreshSec) {
                    Start-Sleep -Milliseconds 100
                }
            } catch {
                $StateData.Error = "$($_.Exception | Out-String)"
                $StateData.Connection.Close()
                $StateData.Reset = $True
            } finally {
                # Wait before we refresh data again
                while ( ((Get-Date) - $StateData.Updated).TotalSeconds -le $StateData.DataRefreshSec) { Start-Sleep -Milliseconds 100 }
            }
            $StateData.Connection.Close()
        }
    })

    # Start the data refresh process
    $newRunspace = [runspacefactory]::CreateRunspace()
    $newRunspace.ThreadOptions = "ReuseThread"         
    $newRunspace.Open()
    $newRunspace.SessionStateProxy.SetVariable("StateData",$StateData)
    $DataRefreshCmd.Runspace = $newRunspace

    $DataRefresh = $DataRefreshCmd.BeginInvoke()
    Write-Host "Starting background data capture..." -NoNewline

    while ( -not $StateData.HasNewData -and -not $StateData.Error ) {
        Write-Host "." -NoNewline
        Start-Sleep -Milliseconds 500
    }

    # Start the UI loop
    while(1){
        try {
            # clear the screen
            Clear-Host

            # Calculate how old the results are
            $UpdateAge = ((Get-Date) - ($StateData.Updated)).TotalSeconds
            
            # Track when rendering started, this is displayed to the user
            $RenderStart = Get-Date

            # Lock the statedata, this pauses the data refresh while the UI is rendering to prevent the refresh thread from updating the data while it is being drawn
            $StateData.Lock = $True
            $max_display = $Host.UI.RawUI.WindowSize.Height - 16
            if ( $Debug ) { $max_display = $max_display - 20 }
            if ( $UpdateAge -gt 20 ) { $SlowUpdates = $True } else { $SlowUpdates = $False } 
            $process_count = ($StateData.Results | Measure-Object).Count
            # -------- DRAW THE HEADER -------- #
            Write-Host "           Instance: $($StateData.SqlInstance)$(if ( -not $IsLinux ){", CPU: $($StateData.cpu)%"})"
            Write-Host "          Processes: captured - $($process_count) $(if ($max_display -lt $($process_count)) { ", displaying - $($max_display)" } ), blocking - $($StateData.Results | Where-Object { $_.block -gt 0 } | Measure-Object | Select-Object -ExpandProperty Count)"
            Write-Host "            Updated: " -NoNewline
            Write-Host "$($StateData.Updated) ($([int]($UpdateAge)) seconds ago)$(if ( $SlowUpdates ) { ' - Query is taking longer than it should' } else { '' })" -ForegroundColor "$(if ( $SlowUpdates ) { 'Red' } else { 'Green' })"
            Write-Host "  Data Refresh Rate: " -NoNewline
            Write-Host "$($StateData.DataRefreshSec) sec." -ForegroundColor "$(if ( $StateData.DataRefreshSec -ne $StateData.DataRefreshDefaultSec ) { "yellow" } else { "green" } )"
            Write-Host "   Highlighted Text: $($filter)"
            Write-Host "        Filter Spid: $($StateData.SpidFilter)"
            Write-Host "$("MODE: $($StateData.DisplayMode.ToUpper()) $($SubDisplayMode)".PadRight($Host.UI.RawUI.WindowSize.Width))`n" -BackgroundColor Green -ForegroundColor Black -NoNewline
            # --------------------------------- #
            $ResultString = ""

            # -------- WRITE OUT DEBUG DATA -------- #
            if ( $Debug ) { Write-Host "*** DEBUG DATA ***" -BackgroundColor Red }
            if ( $Debug ) { $StateData | Out-String; $DataRefresh | Out-String;}
            # -------------------------------------- #

            # -------- WRITE OUT RESULTS -------- #
            # Default message if the results are empty
            if (-not $StateData.Results) { Write-Host "No sessions/blockers found, or waiting for additional data..."; $StateData.Lock = $False }

            # Set the sort and display options
            $SortOpt = $SortOptions["$($StateData.DisplayMode)"]
            $DisplayOpt = $DisplayColumns["$($StateData.DisplayMode)$($SubDisplayMode)"]

            # If there are errors, display them
            if ( $StateData.Error ) {
                Write-Host "Error:`n$($StateData.Error)" -ForegroundColor Red
                $StateData.Lock = $False
                $StateData.Reset = $True
            } else {
                # Depending on the display mode we change how the results are processed
                $(if ( $StateData.DisplayMode -eq 'Resources by Program' ) {
                    $StateData.Results | Where-Object { $_.program_name -ne '' } | Group-Object -Property program_name | ForEach-Object { 
                        $spid_count = ($_.Group | Select-Object -Property spid -Unique | Measure-Object).Count
                        $worker_count = $_.Count
                        $app = $_.Name
                        $cpu = 0
                        [float]$lread_mb = 0
                        [float]$tempdb_mb = 0
                        $blocked = 0
                        $_.Group | ForEach-Object {
                            $cpu += $_.cpu
                            $lread_mb += $_.lread_mb
                            $tempdb_mb += $_.tempdb_mb
                            
                            if ( $_.block -ne 0 ) { $blocked++ }
                        }
                        [PSCustomObject]@{
                            'program_name' = $app
                            'spids' = $spid_count
                            'workers' = $worker_count
                            'blocked' = $blocked
                            'total_cpu' = $cpu
                            'total_l_reads_mb' = $lread_mb
                            'total_tempdb_mb' = $tempdb_mb
                        }
                    }
                } elseif ( $StateData.DisplayMode -eq 'Object Tracking' ) {
                    $StateData.Results | Group-Object -Property proc_name | ForEach-Object {
                        [PSCustomObject]@{
                            'object' = $_.Name
                            'workers' = $_.Count
                            'blocked' = ($_.Group | Where-Object { $_.block -ne 0 }).Count
                            'cpu' = ($_.Group | Measure-Object -Sum -Property cpu).Sum
                            'lread_mb' = ($_.Group | Measure-Object -Sum -Property lread_mb).Sum
                            'mem_mb' = ($_.Group | Measure-Object -Sum -Property mem_mb).Sum
                            'tempdb_mb' = ($_.Group | Measure-Object -Sum -Property tempdb_mb).Sum
                        }
                    }
                } elseif ($SubDisplayMode -eq '_summary' -and $StateData.DisplayMode -eq 'Waits') {
                    $StateData.Results | Group-Object -Property wt_type | ForEach-Object {
                        $wait_type = $_.Name
                        $spid_count = $_.Count
                        $wait_time = 0
                        $_.Group | ForEach-Object {
                            $wait_time += $_.wt_ms
                        }
                        [PSCustomObject]@{
                            'wait_type' = $wait_type
                            'spid_count' = $spid_count
                            'total_wait_ms' = $wait_time
                        }

                    }
                } elseif ( $StateData.SpidFilter ) {
                    if ( $StateData.HasNewData ) {
                        $SpidHistory += $StateData.Results | Where-Object { $_.spid -eq $StateData.SpidFilter }
                    }
                    if ( $ShowSpidHistory ) {
                        $DisplayOpt = $DisplayColumns['SpidHistory']
                        $SortOpt = $SortOptions['SpidHistory']
                        $SpidHistory
                    } else {
                        $StateData.Results | Where-Object { $_.spid -eq $StateData.SpidFilter -or $StateData.DisplayMode -eq 'Blocking' }
                    }
                } else {
                    $StateData.Results
                }) | Sort-Object -Property $SortOpt | Select-Object -First $max_display | `
                    Format-Table -Property $DisplayOpt -Wrap | Out-String -Width $Host.UI.RawUI.WindowSize.Width -Stream | ForEach-Object {
                        # Handle special coloring here
                        $Row += 1
                        if ( $filter -and -not $StateData.SpidFilter -and $_.ToLower().Contains("$($filter.ToLower())") ) {
                            $ResultString += "$(color $_ "black" "white")`n"
                        } elseif ( $Row % 2 -eq 1 ) { 
                            $ResultString += "$(color $_ "cyan" "default")`n"
                        } else {
                            $ResultString += "$($_)`n"
                        }
                    }
                $Row = 0
                $ResultString
                $Host.UI.RawUI.WindowTitle = "SQLTop - $($StateData.SqlInstance) - Processes: $($StateData.Results.Count) Blocked: $($StateData.Results | Where-Object { $_.block -gt 0 } | Measure-Object | Select-Object -ExpandProperty Count) CPU: $($StateData.cpu)% - Updated: $($StateData.Updated)"
            }

            # If we are using a spid filter and the inputbuffer property has data in it, display the input buffer for the given spid
            if ( $StateData.InputBuffer -and $StateData.SpidFilter ) {
                Write-Host "SQL STATEMENT".PadRight($Host.UI.RawUI.WindowSize.Width) -ForegroundColor Black -BackgroundColor Yellow
                Write-Host $StateData.InputBuffer -ForegroundColor Yellow
            }

            if ( $StateData.PlanMessage -and $StateData.SpidFilter ) {
                Write-Host "`n$($StateData.PlanMessage)" -ForegroundColor Black -BackgroundColor Cyan
                $StateData.PlanMessage = $null
                # Pause so the user can copy the plan path
                Pause
            }

            # We are done rendering so we lift the refresh lock
            $StateData.HasNewData = $False
            $StateData.Lock = $False            

            # Display data refresh and UI render timings, this can be useful for troubleshooting
            Write-Host "Timings (ms) - Query: $($StateData.QueryTiming), Render: $(((Get-Date) - $RenderStart).TotalMilliseconds)" -ForegroundColor DarkGreen

            # Help
            if ( $StateData.DisplayMode -eq 'Waits' ) {
                Write-Host "Waits Commands: [$(color "s" "green")]pid to track/[$(color "c" "green")]ummulative wait stats toggle/[$(color "p" "green")]ause output/display [$(color "m" "green")]ode/[$(color "q" "green")]uit/[$(color "C" "green")]hange server`n> " -NoNewline
            } elseif ( $StateData.DisplayMode -eq "Resource Usage" -and $SubDisplayMode -eq "_spid_track") {
                Write-Host "Commands: [$(color "h" "green")]istory toggle/[$(color "g" "green")]et plan/[$(color "t" "green")]ext to highlight/[$(color "p" "green")]ause output/display [$(color "m" "green")]ode/[$(color "q" "green")]uit/[$(color "C" "green")]hange server/[$(color "K" "red")]ILL SPID!`n> " -NoNewline
            } else {
                Write-Host "Commands: [$(color "s" "green")]pid to track/[$(color "t" "green")]ext to highlight/[$(color "p" "green")]ause output/display [$(color "m" "green")]ode/[$(color "q" "green")]uit/[$(color "C" "green")]hange server`n> " -NoNewline
            }
            
            # Loop for the interval defined in UI_refresh_sec
            $StartSleep = Get-Date
            while ( ((Get-Date) - $StartSleep).TotalSeconds -le $UI_refresh_sec ) {
                # If the user pressed a key, capture it and process the users choice
                if($Host.UI.RawUI.KeyAvailable) {
                    $key = $($host.UI.RawUI.ReadKey("NoEcho,IncludeKeyUp")).character
                    if ( $key -eq 'D' ) {
                        # Switch to debug mode
                        $Debug = -not $Debug
                        $StateData.Debug = -not $StateData.Debug
                        break
                    } elseif ( $key -eq '?' ) {
                        Clear-Host
                        Write-Host "SQLTOP HELP"
                        Write-Host "=".PadLeft($Host.UI.RawUI.WindowSize.Width,'=')
                        Write-Host @"
SHORTCUT Keys:
'?' - Gets to this help document
'd' - Enters debug mode
'q' - Exits the app, if you are in debug mode it will take you out of debug mode, if you are filtering it will clear the filter.
't' - Filter results on arbitrary text
's' - Filter results to a specific spid (this mode will also track changes in CPU usage and logical reads)
'm' - Allows you to switch to different display modes
        'b' - Blocking mode will display a blocking tree for the current instance
        'w' - Wait mode will show what each session is waiting on
        'p' - Resources by Program mode will display aggregate CPU usage and logical reads per unique program name
'p' - Pauses the refresh of the output and waits for you to press enter to continue
'C' - Connects to a different server

NOTES:
By default, data is refreshed every 5 seconds, and the UI is refreshed every 5 seconds. If the query takes longer than 10 seconds to execute the data refresh time will be increased. This will continue to happen until the query sucessfully completes. At that point the refresh interval will be reduced until it gets back to the default of 5 seconds.

SQLTOP utilizes the sys.sysprocesses DMV. This is one of the only ways you can get useful information on Query Store related background processes, and it tends to show more accurate CPU usage information.
"@
                        Pause
                        break
                    } else {
                        if ( $key -eq 's' ) {
                            $StateData.SpidFilter = Read-Host "`nspid to track "
                            $SpidHistory = @()
                            if ( $StateData.SpidFilter ) {
                                $SubDisplayMode = '_spid_track'
                            } else {
                                $SubDisplayMode = ''
                            }
                            break
                        } elseif ( $StateData.SpidFilter -and $key -eq 'h') {
                            if ( $ShowSpidHistory ) {
                                $ShowSpidHistory = $False
                            } else {
                                $ShowSpidHistory = $True
                            }
                            break
                        } elseif ( $StateData.DisplayMode -eq "Waits" -and $key -ceq 'c' ) { 
                            if ( $SubDisplayMode -eq "_summary" ) {
                                $SubDisplayMode = $null
                            } else {
                                $SubDisplayMode = "_summary"
                            }
                            break
                        } elseif ( $key -eq 'q' ) {
                            Write-Host "`nExiting..." -ForegroundColor Red
                            $StateData.Run = $False
                            $DataRefresh = $null
                            $Host.UI.RawUI.WindowTitle = $OldTitle
                            return
                        } elseif ( $key -eq 't' ) {
                            $filter = Read-Host "`ntext to track "
                            break
                        } elseif ( $key -eq 'p' ) {
                            Write-Host "PAUSED" -ForegroundColor White -BackgroundColor Red
                            Pause
                            break
                        } elseif ( $key -eq 'm') {
                            $mode = Read-Host "`nSwitch to [$(color "w" "green")]aits/[$(color "r" "green")]esource usage/[$(color "b" "green")]locking mode/resource usage per [$(color "p" "green")]rogram/[$(color "o" "green")]bject view"
                            switch ($mode) {
                                w { $StateData.DisplayMode ='Waits' }
                                r { $StateData.DisplayMode ='Resource Usage' }
                                b { $StateData.DisplayMode ='Blocking'; $StateData.Results = $null }
                                p { $StateData.DisplayMode ='Resources by Program' }
                                o { $StateData.DisplayMode ='Object Tracking' }
                                Default { $StateData.DisplayMode ='Resource Usage' }
                            }
                            $SubDisplayMode = $null
                            break
                        } elseif ( $key -ceq 'C' ) {
                            $NewSqlInstance = Read-Host "`nEnter SQL instance to connect to"
                            if ( $NewSqlInstance -and (Invoke-Sqlcmd -Server $NewSqlInstance -Query "SELECT 1" -ConnectionTimeout 5 -ErrorAction Continue) ) {
                                $StateData.SqlInstance = $NewSqlInstance
                                $StateData.Reset = $True
                            } else {
                                Write-Host "Could not connect to new host..."
                                pause
                            }
                            break
                        } elseif ( $SubDisplayMode -eq "_spid_track" ) {
                            if ( $key -eq 'g' ) {
                                Write-Host "Attempting to get plan..."
                                $StateData.GetPlan = $True
                            } elseif ( $key -ceq 'K') {
                                Write-Host "KILLING SPID" -ForegroundColor White -BackgroundColor Red
                                $Choice = Read-Host "Are you sure you want to kill spid $($StateData.SpidFilter)? [y/n]"

                                if ( $Choice -eq 'y' ) {
                                    $QuerySplat = @{
                                        ServerInstance = $StateData.SqlInstance
                                        Query = "KILL $($StateData.SpidFilter)"
                                        Database = "Master"
                                        MaxCharLength = 9999999
                                    }
                    
                                    if ( $SqlAuth ) {
                                        $QuerySplat.Add("UserName","$($StateData.UserName)")
                                        $QuerySplat.Add("Password","$($StateData.Password)")
                                    }

                                    try {
                                        Invoke-Sqlcmd @QuerySplat
                                        Write-Host "KILLED!" -ForegroundColor White -BackgroundColor Red
                                        Pause
                                    } catch {
                                        Write-Host "Unable to kill spid: $_.Exception.Message"
                                        Pause
                                    }
                                }
                            }
                        }
                    }
                    continue
                }
                # This reduces CPU load on the client by adding a small delay in our while loop
                Start-Sleep -Milliseconds 50

                # Null out our key
                $key = $null
            }
        } catch {
            $Host.UI.RawUI.WindowTitle = $OldTitle
            Write-Error "$($_.Exception | Select-Object * | Out-String)" -ErrorAction Stop
        }
    }
}