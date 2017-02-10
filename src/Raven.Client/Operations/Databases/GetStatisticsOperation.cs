using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Sparrow.Json;

namespace Raven.NewClient.Operations.Databases
{
    public class GetStatisticsOperation : IAdminOperation<DatabaseStatistics>
    {
        public RavenCommand<DatabaseStatistics> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetStatisticsCommand();
        }
    }
}