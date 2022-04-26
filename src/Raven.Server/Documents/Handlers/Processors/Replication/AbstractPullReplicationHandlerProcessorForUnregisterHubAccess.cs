using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractPullReplicationHandlerProcessorForUnregisterHubAccess<TRequestHandler> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler>
        where TRequestHandler : RequestHandler
    {
        private string _hub;
        private string _thumbprint;
        private string _databaseName;

        protected AbstractPullReplicationHandlerProcessorForUnregisterHubAccess([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration, JsonOperationContext context)
        {
            _databaseName = GetDatabaseName();
            _hub = RequestHandler.GetStringQueryString("name", true);
            _thumbprint = RequestHandler.GetStringQueryString("thumbprint", true);

            if (ResourceNameValidator.IsValidResourceName(_databaseName, RequestHandler.ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            RequestHandler.ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var command = new UnregisterReplicationHubAccessCommand(_databaseName, _hub, _thumbprint, raftRequestId);
            return await RequestHandler.Server.ServerStore.SendToLeaderAsync(command);
        }
    }
}
