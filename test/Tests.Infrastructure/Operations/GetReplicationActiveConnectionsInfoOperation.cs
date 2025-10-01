using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Replication;
using Raven.Server.Documents.Handlers.Processors.Replication;
using Sparrow.Json;

namespace Tests.Infrastructure.Operations;

public class GetReplicationActiveConnectionsInfoOperation : IMaintenanceOperation<ReplicationActiveConnectionsPreview>
{
    public RavenCommand<ReplicationActiveConnectionsPreview> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetReplicationActiveConnectionsInfoCommand();
    }
}
