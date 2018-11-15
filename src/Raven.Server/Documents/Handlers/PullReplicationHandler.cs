using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Security;
using Raven.Client.Json.Converters;
using Raven.Server.Extensions;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class PullReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/tasks/pull-replication", "PUT", AuthorizationStatus.Operator)]
        public async Task DefinePullReplication()
        {
            if (ResourceNameValidator.IsValidResourceName(Database.Name, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
                throw new BadRequestException(errorMessage);

            ServerStore.LicenseManager.AssertCanAddPullReplication();
            await DatabaseConfigurations((_, databaseName, blittableJson) =>
                {
                    var pullReplication = JsonDeserializationClient.PullReplicationDefinition(blittableJson);
                    pullReplication.Validate();
                    var updatePullReplication = new UpdatePullReplicationCommand(databaseName)
                    {
                        Definition = pullReplication
                    };
                    return ServerStore.SendToLeaderAsync(updatePullReplication);
                }, "update_pull_replication",
                fillJson: (json, _, index) =>
                {
                    json[nameof(OngoingTask.TaskId)] = index;
                }, statusCode: HttpStatusCode.Created);
        }

        [RavenAction("/databases/*/admin/certificates/feature", "PUT", AuthorizationStatus.Operator)]
        public Task PutFeatureCertificateOnEdge()
        {
            return PutCertificate(PullReplicationDefinition.GetPrefix(Database.Name), includePrivateKey: true);
        }

        [RavenAction("/databases/*/info/remote-task/tcp", "GET", AuthorizationStatus.RestrictedAccess)]
        public Task GetRemoteTaskTcp()
        {
            var remoteTask = GetStringQueryString("remote-task");
            var database = GetStringQueryString("database");
            Authenticate(HttpContext, ServerStore, database, remoteTask);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var output = Server.ServerStore.GetTcpInfoAndCertificates(HttpContext.Request.GetClientRequestedNodeUrl());
                context.Write(writer, output);
            }

            return Task.CompletedTask;
        }

        public static void Authenticate(HttpContext httpContext, ServerStore serverStore, string database, string remoteTask)
        {
            // TODO: think of some use cases like:
            // 1. HTTP, but we defined cert.
            // 2. HTTPs, but no cert is defined.
            // 3. HTTPs with some cert, but got the server cert.
            var feature = httpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            switch (feature?.Status)
            {
                case RavenServer.AuthenticationStatus.Allowed:
                case RavenServer.AuthenticationStatus.Operator:
                case RavenServer.AuthenticationStatus.ClusterAdmin:
                    // we can trust this certificate
                    return;

                case null:
                case RavenServer.AuthenticationStatus.UnfamiliarCertificate:
                    using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var pullReplication = serverStore.Cluster.ReadPullReplicationDefinition(database, remoteTask, context);
                        var cert = httpContext.Connection.ClientCertificate;
                        if (pullReplication.CanAccess(cert?.Thumbprint, out var err) == false)
                        {
                            throw new AuthorizationException(err);
                        }
                    }
                    return;

                default:
                    throw new ArgumentException($"This is a bug, we should deal with '{feature?.Status}' authentication status at RequestRoute.TryAuthorize function.");
            }
        }
    }
}
