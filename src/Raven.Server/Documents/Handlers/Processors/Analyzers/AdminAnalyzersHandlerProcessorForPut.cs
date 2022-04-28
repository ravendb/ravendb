using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Analyzers
{
    internal class AdminAnalyzersHandlerProcessorForPut : AbstractAdminAnalyzersHandlerProcessorForPut<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AdminAnalyzersHandlerProcessorForPut([NotNull] DatabaseRequestHandler requestHandler)
            : base(requestHandler)
        {
        }
    }
}
