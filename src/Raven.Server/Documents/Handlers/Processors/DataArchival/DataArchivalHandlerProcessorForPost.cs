using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.DataArchival
{
    internal class DataArchivalHandlerProcessorForPost : AbstractDataArchivalHandlerProcessorForPost<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public DataArchivalHandlerProcessorForPost([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
