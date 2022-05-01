using System;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Json.Serialization;
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

        protected override ValueTask AssertCanExecuteAsync(string databaseName)
        {
            RequestHandler.ServerStore.LicenseManager.AssertCanAddPullReplicationAsHub();

            return base.AssertCanExecuteAsync(databaseName);
        }

        protected override async ValueTask<BlittableJsonReaderObject> GetConfigurationAsync(TransactionOperationContext context, string databaseName, AsyncBlittableJsonTextWriter writer)
        {
            _hubTaskName = RequestHandler.GetStringQueryString("name", true);

            using (context.OpenReadTransaction())
            {
                _hubDefinition = RequestHandler.Server.ServerStore.Cluster.ReadPullReplicationDefinition(databaseName, _hubTaskName, context);
            }

            if (_hubDefinition == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return null;
            }

#pragma warning disable CS0618 // Type or member is obsolete
            if (_hubDefinition.Certificates != null && _hubDefinition.Certificates.Count > 0)
#pragma warning restore CS0618 // Type or member is obsolete
            {
                // handle backward compatibility
                throw new InvalidOperationException("Cannot register hub access to a replication hub that already has inline certificates: " + _hubTaskName +
                                                    ". Create a new replication hub and try again");
            }

            return await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "register-hub-access");
        }

        protected override async Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, string databaseName, BlittableJsonReaderObject configuration, string raftRequestId)
        {

            var access = JsonDeserializationClient.ReplicationHubAccess(configuration);
            access.Validate(_hubDefinition.WithFiltering);

            using var cert = new X509Certificate2(Convert.FromBase64String(access.CertificateBase64));

            var command = new RegisterReplicationHubAccessCommand(databaseName, _hubTaskName, access, cert, raftRequestId);
            return await RequestHandler.Server.ServerStore.SendToLeaderAsync(command);
        }
    }
}
