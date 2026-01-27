using System;
using System.Net.Http;
using FastTests;
using Raven.Client.Http;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_25381 : RavenTestBase
{
    public RavenDB_25381(ITestOutputHelper output) : base(output)
    {
    }

    private static AlertRaised GetSampleAlert(string databaseName, string title, string message, AlertType reason)
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

    private static PerformanceHint GetSamplePerformanceHint(string databaseName, string title, string message, PerformanceHintType reason, string source)
    {
        return PerformanceHint.Create(databaseName, title, message, reason, NotificationSeverity.Info, source);
    }

    private static void CreateNotifications(RavenServer server, DocumentDatabase database)
    {
        var serverReplicationAlert = GetSampleAlert(null, "ServerAlert", "This is a server alert", AlertType.Replication);
        var databaseReplicationAlert = GetSampleAlert(database.Name, "DatabaseAlert", "This is a database alert", AlertType.Replication);

        var serverPagingHint = GetSamplePerformanceHint(null, "ServerHint", "This is a server performance hint", PerformanceHintType.Paging, "source_1");
        var databasePagingHint = GetSamplePerformanceHint(database.Name, "DatabaseHint", "This is a database performance hint", PerformanceHintType.Paging, "source_1");
        var databaseReplicationHint1 = GetSamplePerformanceHint(database.Name, "DatabaseHint", "This is a database replication hint 1", PerformanceHintType.Replication, "source_1");
        var databaseReplicationHint2 = GetSamplePerformanceHint(database.Name, "DatabaseHint", "This is a database replication hint 2", PerformanceHintType.Replication, "source_2");

        server.ServerStore.NotificationCenter.Add(serverReplicationAlert);
        database.NotificationCenter.Add(databaseReplicationAlert);

        server.ServerStore.NotificationCenter.Add(serverPagingHint);
        database.NotificationCenter.Add(databasePagingHint);
        database.NotificationCenter.Add(databaseReplicationHint1);
        database.NotificationCenter.Add(databaseReplicationHint2);
    }
    
    private class GetServerNotificationsCommand : RavenCommand<object>
    {
        public override bool IsReadRequest => true;
        
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/admin/server/notifications";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();

            Result = response;
        }
    }
    
    private class GetDatabaseNotificationsCommand : RavenCommand<object>
    {
        private readonly int _shardNumber;
        private readonly bool _isSharded;
        
        public override bool IsReadRequest => true;
        
        public GetDatabaseNotificationsCommand(bool isSharded = false, int shardNumber = 0)
        {
            _isSharded = isSharded;
            _shardNumber = shardNumber;
        }
        
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            if (_isSharded)
            {
                url = $"{node.Url}/databases/{node.Database}/notifications?nodeTag={node.ClusterTag}&shardNumber={_shardNumber}";
            }
            else
            {
                url = $"{node.Url}/databases/{node.Database}/notifications";
            }
            
            return new HttpRequestMessage
            {
                Method = HttpMethod.Get
            };
        }
        
        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                ThrowInvalidResponse();
        
            Result = response;
        }
    }

    [RavenFact(RavenTestCategory.Monitoring)]
    public void ServerNotificationsShouldBeExtracted()
    {
        DoNotReuseServer();
        
        using (var store = GetDocumentStore())
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();

            CreateNotifications(Server, database);
            
            using (var commands = store.Commands())
            {
                var cmd = new GetServerNotificationsCommand();
                commands.Execute(cmd);

                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);
                
                res.TryGet("TotalResults", out int totalResults);

                Assert.Equal(2, totalResults);

                res.TryGet("Results", out BlittableJsonReaderArray results);

                var alert = (BlittableJsonReaderObject)results[0];
                alert.TryGet("Id", out string alertId);
                Assert.Equal("AlertRaised/Replication/Key", alertId);

                var performanceHint = (BlittableJsonReaderObject)results[1];
                performanceHint.TryGet("Id", out string performanceHintId);
                Assert.Equal("PerformanceHint/Paging/source_1", performanceHintId);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Monitoring)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void ShardedDatabaseNotificationsShouldBeExtracted(Options options)
    {
        const int shardNumber = 1;

        using (var store = GetDocumentStore(options))
        {
            var database = GetDatabase($"{store.Database}${shardNumber}").GetAwaiter().GetResult();

            CreateNotifications(Server, database);

            using (var commands = store.Commands())
            {
                var cmd = new GetDatabaseNotificationsCommand(isSharded: true, shardNumber);
                commands.Execute(cmd);

                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);

                res.TryGet("TotalResults", out int totalResults);
                Assert.Equal(4, totalResults);

                res.TryGet("Results", out BlittableJsonReaderArray results);

                var alert = (BlittableJsonReaderObject)results[0];
                alert.TryGet("Id", out string alertId);
                Assert.Equal("AlertRaised/Replication/Key", alertId);

                var performanceHint1 = (BlittableJsonReaderObject)results[1];
                performanceHint1.TryGet("Id", out string performanceHintId1);
                Assert.Equal("PerformanceHint/Paging/source_1", performanceHintId1);

                var performanceHint2 = (BlittableJsonReaderObject)results[2];
                performanceHint2.TryGet("Id", out string performanceHintId2);
                Assert.Equal("PerformanceHint/Replication/source_1", performanceHintId2);

                var performanceHint3 = (BlittableJsonReaderObject)results[3];
                performanceHint3.TryGet("Id", out string performanceHintId3);
                Assert.Equal("PerformanceHint/Replication/source_2", performanceHintId3);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Monitoring)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public void NonShardedDatabaseNotificationsShouldBeExtracted(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();

            CreateNotifications(Server, database);

            using (var commands = store.Commands())
            {
                var cmd = new GetDatabaseNotificationsCommand();
                commands.Execute(cmd);

                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);

                res.TryGet("TotalResults", out int totalResults);
                Assert.Equal(4, totalResults);

                res.TryGet("Results", out BlittableJsonReaderArray results);

                var alert = (BlittableJsonReaderObject)results[0];
                alert.TryGet("Id", out string alertId);
                Assert.Equal("AlertRaised/Replication/Key", alertId);

                var performanceHint1 = (BlittableJsonReaderObject)results[1];
                performanceHint1.TryGet("Id", out string performanceHintId1);
                Assert.Equal("PerformanceHint/Paging/source_1", performanceHintId1);

                var performanceHint2 = (BlittableJsonReaderObject)results[2];
                performanceHint2.TryGet("Id", out string performanceHintId2);
                Assert.Equal("PerformanceHint/Replication/source_1", performanceHintId2);

                var performanceHint3 = (BlittableJsonReaderObject)results[3];
                performanceHint3.TryGet("Id", out string performanceHintId3);
                Assert.Equal("PerformanceHint/Replication/source_2", performanceHintId3);
            }
        }
    }
}
