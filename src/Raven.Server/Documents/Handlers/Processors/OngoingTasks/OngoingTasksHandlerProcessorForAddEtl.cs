using JetBrains.Annotations;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal sealed class OngoingTasksHandlerProcessorForAddEtl : AbstractOngoingTasksHandlerProcessorForAddEtl<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForAddEtl([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext _, DynamicJsonValue responseJson, BlittableJsonReaderObject configuration, long index)
        {
            base.OnBeforeResponseWrite(_, responseJson, configuration, index);

            switch (EtlConfiguration<ConnectionString>.GetEtlType(configuration))
            {
                case EtlType.GenAi:
                    responseJson[nameof(GenAi.ChangeVector)] = GetChangeVector();
                    break;
                default:
                    return;
            }

        }

        protected override string GetChangeVector()
        {
            var changeVector = base.GetChangeVector();

            if (StartingPointChangeVector.From(changeVector) != StartingPointChangeVector.LastDocument)
                return changeVector;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                return DocumentsStorage.GetFullDatabaseChangeVector(context);
            }
        }
    }
}
