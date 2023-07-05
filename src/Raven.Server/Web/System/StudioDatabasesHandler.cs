using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.Web.System.Processors.Studio;

namespace Raven.Server.Web.System;

public class StudioDatabasesHandler : ServerRequestHandler
{
    [RavenAction("/studio-tasks/databases", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task Databases()
    {
        using (var processor = new StudioDatabasesHandlerForGetDatabases(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/studio-tasks/databases/state", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
    public async Task GetDatabasesState()
    {
        using (var processor = new StudioDatabasesHandlerForGetDatabasesState(this))
            await processor.ExecuteAsync();
    }
}
