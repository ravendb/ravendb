using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal class SampleDataHandlerProcessorForGetSampleDataClasses : AbstractSampleDataHandlerProcessorForGetSampleDataClasses<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public SampleDataHandlerProcessorForGetSampleDataClasses([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }
    }
}
