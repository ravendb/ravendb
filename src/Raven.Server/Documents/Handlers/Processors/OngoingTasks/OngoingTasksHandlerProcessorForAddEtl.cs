using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForAddEtl : AbstractOngoingTasksHandlerProcessorForAddEtl<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForAddEtl([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
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
                    return;
                default:
                    throw new NotSupportedException($"Unknown ETL type {type}");
            }
        }
    }
}
