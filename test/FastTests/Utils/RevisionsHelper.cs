using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Revisions;
using Sparrow.Json;

namespace FastTests.Utils
{
    public class RevisionsHelper
    {
        public static async Task SetupConflictedRevisionsAsync(IDocumentStore store, Raven.Server.ServerWide.ServerStore serverStore, RevisionsCollectionConfiguration configuration)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));
            if (serverStore == null)
                throw new ArgumentNullException(nameof(serverStore));
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var result = await store.Maintenance.Server.SendAsync(new ConfigureRevisionsForConflictsOperation(store.Database, configuration));

            var documentDatabase = await serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(result.RaftCommandIndex.Value, serverStore.Engine.OperationTimeout);
        }

        public static async Task SetupRevisions(IDocumentStore store, Raven.Server.ServerWide.ServerStore serverStore, RevisionsConfiguration configuration)
        {
            if (store == null)
                throw new ArgumentNullException(nameof(store));
            if (serverStore == null)
                throw new ArgumentNullException(nameof(serverStore));
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var result = await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(configuration));

            var documentDatabase = await serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(result.RaftCommandIndex.Value, serverStore.Engine.OperationTimeout);
        }

        public static async Task<long> SetupRevisions(IDocumentStore documentStore, Raven.Server.ServerWide.ServerStore serverStore)
        {
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false
                }
            };

            var database = documentStore.Database;
            var index = await SetupRevisions(serverStore, database, configuration);

            var documentDatabase = await serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, serverStore.Engine.OperationTimeout);

            return index;
        }

        public static async Task<long> SetupRevisions(Raven.Server.ServerWide.ServerStore serverStore, string database, Action<RevisionsConfiguration> modifyConfiguration = null, int minRevisionToKeep = 5)
        {
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = minRevisionToKeep
                },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true,
                        MinimumRevisionsToKeep = 123
                    },
                    ["People"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 10
                    },
                    ["Comments"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = true
                    },
                    ["Products"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = true
                    }
                }
            };

            modifyConfiguration?.Invoke(configuration);

            var index = await SetupRevisions(serverStore, database, configuration);

            var documentDatabase = await serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(database);
            await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, serverStore.Engine.OperationTimeout);

            return index;
        }

        public static async Task<long> SetupRevisions(Raven.Server.ServerWide.ServerStore serverStore, string database, RevisionsConfiguration configuration)
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configurationJson = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration, context);
                var (index, _) = await serverStore.ModifyDatabaseRevisions(context, database, configurationJson, Guid.NewGuid().ToString());
                return index;
            }
        }

        public class RevertRevisionsOperation : IMaintenanceOperation<OperationIdResult>
        {
            private readonly RevertRevisionsRequest _request;

            public RevertRevisionsOperation(DateTime time, long window)
            {
                _request = new RevertRevisionsRequest()
                {
                    Time = time,
                    WindowInSec = window,
                };
            }

            public RevertRevisionsOperation(DateTime time, long window, string[] collections) : this(time, window)
            {
                if (collections == null || collections.Length == 0)
                    throw new InvalidOperationException("Collections array cant be null or empty.");
                _request.Collections = collections;
            }

            public RevertRevisionsOperation(RevertRevisionsRequest request)
            {
                _request = request ?? throw new ArgumentNullException(nameof(request));
            }

            public RavenCommand<OperationIdResult> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new RevertRevisionsCommand(_request);
            }

            private class RevertRevisionsCommand : RavenCommand<OperationIdResult>
            {
                private readonly RevertRevisionsRequest _request;

                public RevertRevisionsCommand(RevertRevisionsRequest request)
                {
                    _request = request;
                }

                public override bool IsReadRequest => false;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/revisions/revert";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Post,
                        Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(_request, ctx)).ConfigureAwait(false))
                    };
                }

                public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
                {
                    if (response == null)
                        ThrowInvalidResponse();

                    Result = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<OperationIdResult>(response);
                }
            }
        }


    }
}
