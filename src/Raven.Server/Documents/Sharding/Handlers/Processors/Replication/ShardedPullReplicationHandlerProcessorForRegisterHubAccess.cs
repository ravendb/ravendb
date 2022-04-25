using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Replication;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Replication
{
    internal class ShardedPullReplicationHandlerProcessorForRegisterHubAccess : AbstractPullReplicationHandlerProcessorForRegisterHubAccess<ShardedDatabaseRequestHandler>
    {
        public ShardedPullReplicationHandlerProcessorForRegisterHubAccess([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;

        protected override ValueTask WaitForIndexNotificationAsync(long index) => RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index);
    }
}
