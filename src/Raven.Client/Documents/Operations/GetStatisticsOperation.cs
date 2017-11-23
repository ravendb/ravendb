using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations
{
    public class GetStatisticsOperation : IMaintenanceOperation<DatabaseStatistics>
    {
        public RavenCommand<DatabaseStatistics> GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new GetStatisticsCommand();
        }
    }
}
