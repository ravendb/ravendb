using System;
using System.Collections.Generic;
using System.Net.Http;
using FastTests;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client.Http;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25381 : RavenTestBase
{
    public RavenDB_25381(ITestOutputHelper output) : base(output)
    {
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
        private readonly int _pageSize;

        public override bool IsReadRequest => true;

        public GetDatabaseNotificationsCommand(bool isSharded = false, int shardNumber = 0, int pageSize = int.MaxValue)
        {
            _isSharded = isSharded;
            _shardNumber = shardNumber;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var baseUrl = $"{node.Url}/databases/{node.Database}/notifications";

            if (_isSharded)
            {
                var queryParams = new Dictionary<string, string>
                {
                    { "nodeTag", node.ClusterTag },
                    { "shardNumber", _shardNumber.ToString() },
                    { "pageSize", _pageSize.ToString() }
                };

                url = QueryHelpers.AddQueryString(baseUrl, queryParams);
            }
            else
            {
                var queryParams = new Dictionary<string, string>
                {
                    { "pageSize", _pageSize.ToString() }
                };

                url = QueryHelpers.AddQueryString(baseUrl, queryParams);
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

    [RavenTheory(RavenTestCategory.Monitoring)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public void QueryParametersShouldBeRespected(Options options)
    {
        const int pageSize = 1;

        using (var store = GetDocumentStore(options))
        {
            var database = GetDatabase(store.Database).GetAwaiter().GetResult();

            CreateNotifications(Server, database);

            using (var commands = store.Commands())
            {
                var cmd = new GetDatabaseNotificationsCommand(pageSize: pageSize);
                commands.Execute(cmd);

                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);

                res.TryGet("TotalResults", out int totalResults);
                Assert.Equal(4, totalResults);

                res.TryGet("Results", out BlittableJsonReaderArray results);

                Assert.Equal(pageSize, results.Length);

                var alert = (BlittableJsonReaderObject)results[0];
                alert.TryGet("Id", out string alertId);
                Assert.Equal("AlertRaised/Replication/Key", alertId);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Monitoring)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
    public void QueryParametersShouldBeRespectedInShardedHandler(Options options)
    {
        const int shardNumber = 1;
        const int pageSize = 1;

        using (var store = GetDocumentStore(options))
        {
            var database = GetDatabase($"{store.Database}${shardNumber}").GetAwaiter().GetResult();

            CreateNotifications(Server, database);

            using (var commands = store.Commands())
            {
                var cmd = new GetDatabaseNotificationsCommand(isSharded: true, shardNumber, pageSize);
                commands.Execute(cmd);

                var res = cmd.Result as BlittableJsonReaderObject;

                Assert.NotNull(res);

                res.TryGet("TotalResults", out int totalResults);
                Assert.Equal(4, totalResults);

                res.TryGet("Results", out BlittableJsonReaderArray results);

                Assert.Equal(pageSize, results.Length);

                var alert = (BlittableJsonReaderObject)results[0];
                alert.TryGet("Id", out string alertId);
                Assert.Equal("AlertRaised/Replication/Key", alertId);
            }
        }
    }
}
