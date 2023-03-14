using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForAddEtl : AbstractOngoingTasksHandlerProcessorForAddEtl<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForAddEtl([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void AssertCanAddOrUpdateEtl(ref BlittableJsonReaderObject etlConfiguration)
        {
            if(EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration) == EtlType.Queue)
                throw new NotSupportedInShardingException("Queue ETLs are currently not supported in sharding");

            base.AssertCanAddOrUpdateEtl(ref etlConfiguration);
        }
    }
}
