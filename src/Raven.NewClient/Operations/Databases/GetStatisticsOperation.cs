using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;

namespace Raven.NewClient.Operations.Databases
{
    public class GetStatisticsOperation : IAdminOperation<DatabaseStatistics>
    {
        public RavenCommand<DatabaseStatistics> GetCommand(DocumentConvention conventions)
        {
            return new GetStatisticsCommand();
        }
    }
}