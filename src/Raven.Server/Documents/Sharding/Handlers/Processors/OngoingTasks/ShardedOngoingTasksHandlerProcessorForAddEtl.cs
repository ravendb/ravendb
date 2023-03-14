using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.OngoingTasks;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForAddEtl : AbstractOngoingTasksHandlerProcessorForAddEtl<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedOngoingTasksHandlerProcessorForAddEtl([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void AssertIsEtlTypeSupported(EtlType type)
        {
            switch (type)
            {
                case EtlType.Raven:
                case EtlType.Sql:
                case EtlType.Olap:
                case EtlType.ElasticSearch:
                case EtlType.Queue:
                    throw new NotSupportedInShardingException("Queue ETLs are currently not supported in sharding");
                default:
                    throw new NotSupportedException($"Unknown ETL type {type}");
            }
        }
    }
}
