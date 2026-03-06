using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Server;
using Raven.Server.Config.Settings;
using Raven.Server.Dashboard.Cluster;
using Raven.Server.Dashboard.Cluster.Notifications.DatabaseNotifications;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Binary;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SlowTests.Issues;

public class RavenDB_24424 : RavenTestBase
{
    public RavenDB_24424(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Monitoring)]
    [InlineData("SchemaUpgrade/Issues/SystemVersion/RavenDB-24424_pre_schema_upgrade.zip")]
    private void NotificationsFromOldSchemaShouldBeMigrated(string filePath)
    {
        var folder = NewDataPath(forceCreateDir: true, prefix: Guid.NewGuid().ToString());
            
        var zipPath = new PathSetting(filePath);
        Assert.True(File.Exists(zipPath.FullPath));
            
        ZipFile.ExtractToDirectory(filePath, folder);

        using (var server = GetNewServer(new ServerCreationOptions { DeletePrevious = false, RunInMemory = false, DataDirectory = folder }))
        {
            var storeOptions = new Options() { Server = server, ModifyDatabaseName = _ => "TestBackup_1"};

            using (var store = GetDocumentStore(storeOptions))
            {
                var database = GetDatabase(store.Database, server).Result;
                    
                using (database.NotificationCenter.GetStored(out var databaseActions))
                {
                    var notificationTableValues = databaseActions.ToList();
                    Assert.Equal(4, notificationTableValues.Count);

                    Assert.NotEqual(-1, notificationTableValues.ToList()[0].Reason);
                    Assert.NotEqual(-1, notificationTableValues.ToList()[1].Reason);
                    Assert.NotEqual(-1, notificationTableValues.ToList()[2].Reason);
                    Assert.NotEqual(-1, notificationTableValues.ToList()[3].Reason);
                    
                    Assert.NotEqual(-1, notificationTableValues.ToList()[0].Type);
                    Assert.NotEqual(-1, notificationTableValues.ToList()[1].Type);
                    Assert.NotEqual(-1, notificationTableValues.ToList()[2].Type);
                    Assert.NotEqual(-1, notificationTableValues.ToList()[3].Type);
                }

                using (server.ServerStore.NotificationCenter.GetStored(out var serverActions))
                {
                    var notificationTableValues = serverActions.ToList();
                    
                    // We may store an AGPL notification before reading from notifications storage
                    Assert.True(notificationTableValues.Count >= 2);
                    
                    var notificationsList = notificationTableValues.ToList();
                    var alert = notificationsList.Single(x => x.Json.TryGet("Title", out string title) && title == "ServerAlert");
                    var performanceHint = notificationsList.Single(x => x.Json.TryGet("Title", out string title) && title == "ServerHint");
                    
                    var alertType = (NotificationType)Bits.SwapBytes(alert.Type);
                    var alertReason = (AlertReason)Bits.SwapBytes(alert.Reason);
                    
                    Assert.Equal(NotificationType.AlertRaised, alertType);
                    Assert.Equal(AlertReason.Replication, alertReason);
                    
                    var performanceHintType = (NotificationType)Bits.SwapBytes(performanceHint.Type);
                    var performanceHintReason = (PerformanceHintReason)Bits.SwapBytes(performanceHint.Reason);
                    
                    Assert.Equal(NotificationType.PerformanceHint, performanceHintType);
                    Assert.Equal(PerformanceHintReason.Paging, performanceHintReason);
                    
                    Assert.True(alert.Json.TryGet("Reason", out string alertReasonFromJson));
                    var alertReasonEnumValue = Enum.Parse<AlertReason>(alertReasonFromJson);
                    Assert.Equal(alertReasonEnumValue, AlertReason.Replication);
                    
                    Assert.True(performanceHint.Json.TryGet("Reason", out string performanceHintReasonFromJson));
                    var performanceHintReasonEnumValue = Enum.Parse<PerformanceHintReason>(performanceHintReasonFromJson);
                    Assert.Equal(performanceHintReasonEnumValue, PerformanceHintReason.Paging);
                }
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Monitoring)]
    [InlineData("SchemaUpgrade/Issues/SystemVersion/RavenDB-24424_two_databases.zip")]
    private void NotificationsFromMultipleDatabasesShouldBeMigrated(string filePath)
    {
        var folder = NewDataPath(forceCreateDir: true, prefix: Guid.NewGuid().ToString());
            
        var zipPath = new PathSetting(filePath);
        Assert.True(File.Exists(zipPath.FullPath));
            
        ZipFile.ExtractToDirectory(filePath, folder);

        using (var server = GetNewServer(new ServerCreationOptions { DeletePrevious = false, RunInMemory = false, DataDirectory = folder, RegisterForDisposal = false }))
        {
            var db1 = GetDatabase("DB1", server).Result;
            var db2 = GetDatabase("DB2", server).Result;

            using (db1.NotificationCenter.GetStored(out var databaseActions))
            {
                var notificationTableValues = databaseActions.ToList();
                Assert.Equal(4, notificationTableValues.Count);

                Assert.NotEqual(-1, notificationTableValues.ToList()[0].Reason);
                Assert.NotEqual(-1, notificationTableValues.ToList()[1].Reason);
                Assert.NotEqual(-1, notificationTableValues.ToList()[2].Reason);
                Assert.NotEqual(-1, notificationTableValues.ToList()[3].Reason);

                Assert.NotEqual(-1, notificationTableValues.ToList()[0].Type);
                Assert.NotEqual(-1, notificationTableValues.ToList()[1].Type);
                Assert.NotEqual(-1, notificationTableValues.ToList()[2].Type);
                Assert.NotEqual(-1, notificationTableValues.ToList()[3].Type);
            }

            using (db2.NotificationCenter.GetStored(out var databaseActions))
            {
                var notificationTableValues = databaseActions.ToList();
                Assert.Equal(4, notificationTableValues.Count);

                Assert.NotEqual(-1, notificationTableValues.ToList()[0].Reason);
                Assert.NotEqual(-1, notificationTableValues.ToList()[1].Reason);
                Assert.NotEqual(-1, notificationTableValues.ToList()[2].Reason);
                Assert.NotEqual(-1, notificationTableValues.ToList()[3].Reason);

                Assert.NotEqual(-1, notificationTableValues.ToList()[0].Type);
                Assert.NotEqual(-1, notificationTableValues.ToList()[1].Type);
                Assert.NotEqual(-1, notificationTableValues.ToList()[2].Type);
                Assert.NotEqual(-1, notificationTableValues.ToList()[3].Type);
            }

            using (server.ServerStore.NotificationCenter.GetStored(out var serverActions))
            {
                var notificationTableValues = serverActions.ToList();

                // We may store an AGPL notification before reading from notifications storage
                Assert.True(notificationTableValues.Count >= 2);

                var notificationsList = notificationTableValues.ToList();
                var alert = notificationsList.Single(x => x.Json.TryGet("Title", out string title) && title == "ServerAlert");
                var performanceHint = notificationsList.Single(x => x.Json.TryGet("Title", out string title) && title == "ServerHint");

                var alertType = (NotificationType)Bits.SwapBytes(alert.Type);
                var alertReason = (AlertReason)Bits.SwapBytes(alert.Reason);

                Assert.Equal(NotificationType.AlertRaised, alertType);
                Assert.Equal(AlertReason.Replication, alertReason);

                var performanceHintType = (NotificationType)Bits.SwapBytes(performanceHint.Type);
                var performanceHintReason = (PerformanceHintReason)Bits.SwapBytes(performanceHint.Reason);

                Assert.Equal(NotificationType.PerformanceHint, performanceHintType);
                Assert.Equal(PerformanceHintReason.Paging, performanceHintReason);

                Assert.True(alert.Json.TryGet("Reason", out string alertReasonFromJson));
                var alertReasonEnumValue = Enum.Parse<AlertReason>(alertReasonFromJson);
                Assert.Equal(alertReasonEnumValue, AlertReason.Replication);

                Assert.True(performanceHint.Json.TryGet("Reason", out string performanceHintReasonFromJson));
                var performanceHintReasonEnumValue = Enum.Parse<PerformanceHintReason>(performanceHintReasonFromJson);
                Assert.Equal(performanceHintReasonEnumValue, PerformanceHintReason.Paging);
            }
        }
    }
        
    private static AlertRaised GetSampleAlert(string databaseName, string title, string message, AlertReason reason)
    {
        return AlertRaised.Create(
            databaseName,
            title, 
            message,
            reason,
            NotificationSeverity.Info,
            key: "Key",
            details: new ExceptionDetails(new Exception("Error message")));
    }

    private static PerformanceHint GetSamplePerformanceHint(string databaseName, string title, string message, PerformanceHintReason reason, string source)
    {
        return PerformanceHint.Create(databaseName, title, message, reason, NotificationSeverity.Info, source);
    }
    
    private static void CreateNotifications(RavenServer server, DocumentDatabase database)
    {
        var serverReplicationAlert = GetSampleAlert(null, "ServerAlert", "This is a server alert", AlertReason.Replication);
        var databaseReplicationAlert = GetSampleAlert(database.Name, "DatabaseAlert", "This is a database alert", AlertReason.Replication);
        
        var serverPagingHint = GetSamplePerformanceHint(null, "ServerHint", "This is a server performance hint", PerformanceHintReason.Paging, "source_1");
        var databasePagingHint = GetSamplePerformanceHint(database.Name, "DatabaseHint", "This is a database performance hint", PerformanceHintReason.Paging, "source_1");
        var databaseReplicationHint1 = GetSamplePerformanceHint(database.Name, "DatabaseHint", "This is a database replication hint 1", PerformanceHintReason.Replication, "source_1");
        var databaseReplicationHint2 = GetSamplePerformanceHint(database.Name, "DatabaseHint", "This is a database replication hint 2", PerformanceHintReason.Replication, "source_2");

        server.ServerStore.NotificationCenter.Add(serverReplicationAlert);
        database.NotificationCenter.Add(databaseReplicationAlert);
        
        server.ServerStore.NotificationCenter.Add(serverPagingHint);
        database.NotificationCenter.Add(databasePagingHint);
        database.NotificationCenter.Add(databaseReplicationHint1);
        database.NotificationCenter.Add(databaseReplicationHint2);
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public async Task NotificationsSummaryShouldBeSent()
    {
        DoNotReuseServer();
        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).Result;
            
            CreateNotifications(Server, database);
            
            var serverUrl = Server.WebUrl;

            using (var ws = new ClientWebSocket())
            {
                var uri = new Uri($"{serverUrl.Replace("http", "ws")}/cluster-dashboard/watch?node=A&fromStudio=true");
                await ws.ConnectAsync(uri, CancellationToken.None);
                Assert.Equal(WebSocketState.Open, ws.State);

                var watchCommandRequest = new WatchCommandRequest
                {
                    Command = "watch",
                    Id = 14,
                    Type = nameof(ClusterDashboardNotificationType.DatabasesNotifications),
                    Config = new DatabaseNotificationsSummaryRequestConfig()
                    {
                        Alerts = new NotificationTypeConfig()
                        {
                            IsEnabled = true
                        },
                        PerformanceHints = new NotificationTypeConfig()
                        {
                            IsEnabled = true
                        }
                    }
                };
                
                var watchCommandJson = JsonSerializer.Serialize(watchCommandRequest);
                var buffer = Encoding.UTF8.GetBytes(watchCommandJson);
                await ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
                
                var receiveBuffer = new byte[4096];
                WatchCommandResponse response = null;
                
                Assert.Equal(true, WaitForValue(() =>
                {
                    var result = ws.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), CancellationToken.None).GetAwaiter().GetResult();
                    var responseString = Encoding.UTF8.GetString(receiveBuffer, 0, result.Count);
                    
                    if (responseString.Contains(nameof(WatchCommandResponse.Id)) == false)
                        return false;
                    
                    response = JsonSerializer.Deserialize<WatchCommandResponse>(responseString);

                    if (response.Data?.Type == nameof(ClusterDashboardNotificationType.DatabasesNotifications))
                        return true;
                    
                    return false;
                }, true));

                Assert.Equal(1, response.Data.NotificationsSummary.Count);
                Assert.Equal(1, response.Data.NotificationsSummary.Single().AlertsCount);
                Assert.Equal(3, response.Data.NotificationsSummary.Single().PerformanceHintsCount);

                var alerts = response.Data.NotificationsSummary.Single().Alerts;
                var performanceHints = response.Data.NotificationsSummary.Single().PerformanceHints;
                
                Assert.Equal(1, alerts.Count);
                Assert.Equal(2, performanceHints.Count);

                var replicationAlerts = alerts.Single(x => x.Reason == AlertReason.Replication.ToString());
                var replicationPerformanceHints = performanceHints.Single(x => x.Reason == PerformanceHintReason.Replication.ToString());
                var pagingPerformanceHints = performanceHints.Single(x => x.Reason == PerformanceHintReason.Paging.ToString());
                
                Assert.Equal(1, replicationAlerts.Count);
                Assert.Equal(2, replicationPerformanceHints.Count);
                Assert.Equal(1, pagingPerformanceHints.Count);
                
                watchCommandRequest = new WatchCommandRequest
                {
                    Command = "update-config",
                    Id = 14,
                    Config = new DatabaseNotificationsSummaryRequestConfig()
                    {
                        PerformanceHints = new NotificationTypeConfig()
                        {
                            IsEnabled = true,
                            Reasons = new HashSet<string>() { PerformanceHintReason.Paging.ToString() }
                        }
                    }
                };
                
                watchCommandJson = JsonSerializer.Serialize(watchCommandRequest);
                buffer = Encoding.UTF8.GetBytes(watchCommandJson);
                await ws.SendAsync(new ArraySegment<byte>(buffer), WebSocketMessageType.Text, true, CancellationToken.None);
                
                Assert.Equal(true, WaitForValue(() =>
                {
                    var result = ws.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), CancellationToken.None).GetAwaiter().GetResult();
                    var responseString = Encoding.UTF8.GetString(receiveBuffer, 0, result.Count);
                    
                    if (responseString.Contains(nameof(WatchCommandResponse.Id)) == false)
                        return false;
                    
                    response = JsonSerializer.Deserialize<WatchCommandResponse>(responseString);

                    if (response.Data?.Type == nameof(ClusterDashboardNotificationType.DatabasesNotifications))
                        return true;
                    
                    return false;
                }, true));
                
                Assert.Equal(1, response.Data.NotificationsSummary.Count);

                alerts = response.Data.NotificationsSummary.Single().Alerts;
                performanceHints = response.Data.NotificationsSummary.Single().PerformanceHints;
                
                Assert.Empty(alerts);
                Assert.Equal(1, performanceHints.Count);
                
                pagingPerformanceHints = performanceHints.Single(x => x.Reason == PerformanceHintReason.Paging.ToString());
                
                Assert.Equal(1, pagingPerformanceHints.Count);
            }
        }
    }
    
    private class WatchCommandRequest
    {
        public string Command { get; set; }
        public int Id { get; set; }
        public string Type { get; set; }
        public DatabaseNotificationsSummaryRequestConfig Config { get; set; }
    }

    private class WatchCommandResponse
    {
        public int Id { get; set; }
        public DashboardData Data { get; set; }

        public class DashboardData
        {
            public string Type { get; set; }
            public DateTime Date { get; set; }
            public List<DatabaseNotificationsSummary> NotificationsSummary { get; set; }
        }
    }
}
