using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class PullReplicationHandlerProcessorForRegisterHubAccess : AbstractPullReplicationHandlerProcessorForRegisterHubAccess<DatabaseRequestHandler>
    {
        public PullReplicationHandlerProcessorForRegisterHubAccess([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName() => RequestHandler.Database.Name;

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.Database.ServerStore.Engine.OperationTimeout);
        }
    }
}
