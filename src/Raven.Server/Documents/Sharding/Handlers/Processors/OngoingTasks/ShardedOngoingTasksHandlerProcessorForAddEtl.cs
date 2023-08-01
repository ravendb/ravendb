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
    internal sealed class ShardedOngoingTasksHandlerProcessorForAddEtl : AbstractOngoingTasksHandlerProcessorForAddEtl<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForAddEtl([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void AssertCanAddOrUpdateEtl(ref BlittableJsonReaderObject etlConfiguration)
        {
            if(EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration) == EtlType.Queue)
                throw new NotSupportedInShardingException("Queue ETLs are currently not supported in sharding");

            etlConfiguration.TryGet(nameof(EtlConfiguration<ConnectionString>.Name), out string name);

            if (etlConfiguration.TryGet(nameof(EtlConfiguration<ConnectionString>.MentorNode), out string mentor) && string.IsNullOrEmpty(mentor) == false)
                throw new InvalidOperationException($"Can't add or update ETL {name}. Choosing a mentor node for an ongoing task is not supported in sharding.");

            base.AssertCanAddOrUpdateEtl(ref etlConfiguration);
        }
    }
}
