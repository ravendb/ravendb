using Raven.Client.Commands;
using Raven.Client.Data;
using Raven.Client.Document;
using Sparrow.Json;

namespace Raven.Client.Operations.Databases
{
    public class GetStatisticsOperation : IAdminOperation<DatabaseStatistics>
    {
        public RavenCommand<DatabaseStatistics> GetCommand(DocumentConvention conventions, JsonOperationContext context)
        {
            return new GetStatisticsCommand();
        }
    }
}