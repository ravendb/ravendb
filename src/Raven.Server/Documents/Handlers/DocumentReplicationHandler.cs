using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Linq;
using Raven.Server.Extensions;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentReplicationHandler : DatabaseRequestHandler
    {
		[RavenAction("/databases/*/replication/tombstones", "GET",
			"/databases/{databaseName:string}/replication/tombstones?start={start:int}&take={take:int}")]
		public Task GetAllTombstones()
		{
			var start = GetIntValueQueryString("start", false) ?? 0;
			var take = GetIntValueQueryString("take", false) ?? 1024;

			HttpContext.Response.StatusCode = 200;

			DocumentsOperationContext context;
			using (ContextPool.AllocateOperationContext(out context))
			using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
			using (context.OpenReadTransaction())
			{
				var tombstones = context.DocumentDatabase.DocumentsStorage.GetTombstonesAfter(context, 0, start, take).ToList();
				var array = new DynamicJsonArray();
				foreach (var tombstone in tombstones)
				{
					array.Add(new DynamicJsonValue
					{
						["Key"] = tombstone.Key,
						["Collection"] = tombstone.Collection,
						["Etag"] = tombstone.Etag,
						["DeletedEtag"] = tombstone.DeletedEtag,
						["ChangeVector"] = tombstone.ChangeVector.ToJson()
					});
				}

				context.Write(writer,array);
			}

			return Task.CompletedTask;
		}

		//get conflicts for specified document
		[RavenAction("/databases/*/replication/conflicts", "GET", 
			"/databases/{databaseName:string}/replication/conflicts?docId={documentId:string}")]
		public Task GetReplicationConflictsByDocument()
		{
			var docId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("docId");
			DocumentsOperationContext context;
			using (ContextPool.AllocateOperationContext(out context))
			using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
			using (context.OpenReadTransaction())
			{
				var conflicts = context.DocumentDatabase.DocumentsStorage.GetConflictsFor(context, docId);
				var array = new DynamicJsonArray();
				foreach (var conflict in conflicts)
				{
					array.Add(new DynamicJsonValue
					{
						["Key"] = conflict.Key.String,
						["ChangeVector"] = conflict.ChangeVector.ToJson(),
					});					
				}

				context.Write(writer,array);

				HttpContext.Response.StatusCode = 200;
				return Task.CompletedTask;
			}
		}

		[RavenAction("/databases/*/replication/topology", "GET")]
        public Task GetReplicationTopology()
        {
            // TODO: Remove this, use "/databases/*/topology" isntead
            HttpContext.Response.StatusCode = 404;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/active-connections", "GET")]
        public Task GetReplicationActiveConnections()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var incoming = new DynamicJsonArray();
                foreach (var item in Database.DocumentReplicationLoader.IncomingConnections)
                {
                    incoming.Add(new DynamicJsonValue
                    {
                        ["SourceDatabaseId"] = item.SourceDatabaseId,
                        ["SourceDatabaseName"] = item.SourceDatabaseName,
                        ["SourceMachineName"] = item.SourceMachineName,
                        ["SourceUrl"] = item.SourceUrl
                    });
                }

                var outgoing = new DynamicJsonArray();
                foreach (var item in Database.DocumentReplicationLoader.OutgoingConnections)
                {
                    outgoing.Add(new DynamicJsonValue
                    {
                        ["Url"] = item.Url,
                        ["Database"] = item.Database,
                        ["Disabled"] = item.Disabled,
                        ["IgnoredClient"] = item.IgnoredClient,
                        ["SkipIndexReplication"] = item.SkipIndexReplication,
                        ["SpecifiedCollections"] = item.SpecifiedCollections
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["IncomingConnections"] = incoming,
                    ["OutgoingConnections"] = outgoing
                });
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/outgoing-failures", "GET")]
        public Task GetReplicationOugoingFailureStats()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in Database.DocumentReplicationLoader.OutgoingFailureInfo)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["Url"] = item.Key.Url,
                            ["Database"] = item.Key.Database,
                            ["Disabled"] = item.Key.Disabled,
                            ["IgnoredClient"] = item.Key.IgnoredClient,
                            ["SkipIndexReplication"] = item.Key.SkipIndexReplication,
                            ["SpecifiedCollections"] = item.Key.SpecifiedCollections
                        },
                        ["Value"] = new DynamicJsonValue
                        {
                            ["ErrorCount"] = item.Value.ErrorCount,
                            ["NextTimout"] = item.Value.NextTimout
                        }
                    });
                }

                context.Write(writer, data);
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/incoming-last-activity-time", "GET")]
        public Task GetReplicationIncomingActivityTimes()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in Database.DocumentReplicationLoader.IncomingLastActivityTime)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["SourceDatabaseId"] = item.Key.SourceDatabaseId,
                            ["SourceDatabaseName"] = item.Key.SourceDatabaseName,
                            ["SourceMachineName"] = item.Key.SourceMachineName,
                            ["SourceUrl"] = item.Key.SourceUrl
                        },
                        ["Value"] = item.Value
                    });
                }

                context.Write(writer, data);
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/incoming-rejection-info", "GET")]
        public Task GetReplicationIncomingRejectionInfo()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var statItem in Database.DocumentReplicationLoader.IncomingRejectionStats)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["SourceDatabaseId"] = statItem.Key.SourceDatabaseId,
                            ["SourceDatabaseName"] = statItem.Key.SourceDatabaseName,
                            ["SourceMachineName"] = statItem.Key.SourceMachineName,
                            ["SourceUrl"] = statItem.Key.SourceUrl
                        },
                        ["Value"] = new DynamicJsonArray(statItem.Value.Select(x => new DynamicJsonValue
                                                        {
                                                            ["Reason"] = x.Reason,
                                                            ["When"] = x.When
                                                        }))
                    });
                }

                context.Write(writer,data);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/outgoing-reconnect-queue", "GET")]
        public Task GetReplicationReconnectionQueue()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var queueItem in Database.DocumentReplicationLoader.ReconnectQueue)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Url"] = queueItem.Url,
                        ["Database"] = queueItem.Database,
                        ["Disabled"] = queueItem.Disabled,
                        ["IgnoredClient"] = queueItem.IgnoredClient,
                        ["SkipIndexReplication"] = queueItem.SkipIndexReplication,
                        ["SpecifiedCollections"] = queueItem.SpecifiedCollections						
                    });
                }

                context.Write(writer, data);
            }
            return Task.CompletedTask;
        }
    }
}