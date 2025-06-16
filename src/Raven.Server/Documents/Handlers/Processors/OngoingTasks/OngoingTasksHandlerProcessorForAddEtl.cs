using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal sealed class OngoingTasksHandlerProcessorForAddEtl : AbstractOngoingTasksHandlerProcessorForAddEtl<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForAddEtl([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetChangeVector()
        {
            var changeVector = base.GetChangeVector();

            if (changeVector != nameof(Constants.Documents.GenAiChangeVectorSpecialStates.LastDocument))
                return changeVector;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                return DocumentsStorage.GetFullDatabaseChangeVector(context);
            }
        }
    }
}
