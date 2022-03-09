using System;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Sharding;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal abstract class AbstractStatsHandlerProcessor<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStatsHandlerProcessor([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected IndexInformation[] GetDatabaseIndexesFromRecord()
        {
            if (RequestHandler is ShardedRequestHandler shardedRequestHandler)
            {
                var record = shardedRequestHandler.ShardedContext.DatabaseRecord;
                var indexes = record.Indexes;
                var indexInformation = new IndexInformation[indexes.Count];

                int i = 0;
                foreach (var key in indexes.Keys)
                {
                    var index = indexes[key];

                    indexInformation[i] = new IndexInformation
                    {
                        Name = index.Name,
                        // IndexDefinition includes nullable fields, then in case of null we set to default values
                        State = index.State ?? IndexState.Normal,
                        LockMode = index.LockMode ?? IndexLockMode.Unlock,
                        Priority = index.Priority ?? IndexPriority.Normal,
                        Type = index.Type,
                        SourceType = index.SourceType,
                        IsStale = false // for sharding we can't determine 
                    };

                    i++;
                }

                return indexInformation;
            }

            return null;
        }
    }
}
