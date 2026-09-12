using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.Handlers
{
    public sealed class SqlEtlServerWideHandler : ServerRequestHandler
    {
        [RavenAction("/admin/etl/sql/test-connection", "POST", AuthorizationStatus.Operator)]
        public Task TestConnection() => SqlEtlTestConnectionHelper.ExecuteAsync(this);
    }
}
