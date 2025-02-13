using Raven.Client.Documents.Operations.AI;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Commands.ETL;

namespace Raven.Server.ServerWide.Commands.AI;

public sealed class AddAiIntegrationCommand : AddEtlCommand<AiIntegrationConfiguration, AiConnectionString>
{
    public AddAiIntegrationCommand()
    {
        // for deserialization
    }

    public AddAiIntegrationCommand(AiIntegrationConfiguration configuration, string databaseName, string uniqueRequestId) : base(configuration, databaseName, uniqueRequestId)
    {

    }

    public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
    {
        Add(ref record.AiIntegrations, record, etag);
    }
}
