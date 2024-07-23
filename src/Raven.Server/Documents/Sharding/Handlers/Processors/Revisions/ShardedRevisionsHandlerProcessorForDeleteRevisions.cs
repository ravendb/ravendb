using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForDeleteRevisions : AbstractRevisionsHandlerProcessorForDeleteRevisions<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRevisionsHandlerProcessorForDeleteRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task DeleteRevisions(DeleteRevisionsRequest request, OperationCancelToken token)
        {
            throw new NotImplementedException();
        }
    }
}
