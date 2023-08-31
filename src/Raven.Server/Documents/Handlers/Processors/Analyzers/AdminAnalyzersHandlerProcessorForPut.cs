using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Analyzers
{
    internal sealed class AdminAnalyzersHandlerProcessorForPut : AbstractAdminAnalyzersHandlerProcessorForPut<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminAnalyzersHandlerProcessorForPut([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }
    }
}
