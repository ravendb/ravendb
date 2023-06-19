using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Archival
{
    internal class ArchivalHandlerProcessorForPost : AbstractArchivalHandlerProcessorForPost<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ArchivalHandlerProcessorForPost([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
