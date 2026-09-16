using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Snowflake.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.Web;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Snowflake.Handlers;

public sealed class SnowflakeEtlServerWideHandler : ServerRequestHandler
{
    [RavenAction("/admin/etl/snowflake/test-connection", "POST", AuthorizationStatus.Operator)]
    public Task TestConnection() => SnowflakeEtlTestConnectionHelper.ExecuteAsync(this);
}
