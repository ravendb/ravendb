using System;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractPullReplicationHandlerProcessorForRegisterHubAccess<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private string _hubTaskName;
        private PullReplicationDefinition _hubDefinition;

        protected AbstractPullReplicationHandlerProcessorForRegisterHubAccess([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<BlittableJsonReaderObject> GetConfigurationAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
        {
            _hubTaskName = RequestHandler.GetStringQueryString("name", true);

            using (context.OpenReadTransaction())
            {
                _hubDefinition = RequestHandler.Server.ServerStore.Cluster.ReadPullReplicationDefinition(RequestHandler.DatabaseName, _hubTaskName, context);
            }

            if (_hubDefinition == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return null;
            }

            return await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "register-hub-access");
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {

            var access = JsonDeserializationClient.ReplicationHubAccess(configuration);
            access.Validate(_hubDefinition.WithFiltering);

            using var cert = CertificateHelper.CreateCertificate(Convert.FromBase64String(access.CertificateBase64));

            var command = new RegisterReplicationHubAccessCommand(RequestHandler.DatabaseName, _hubTaskName, access, cert, raftRequestId);
            return await RequestHandler.Server.ServerStore.SendToLeaderAsync(command);
        }
    }
}
