using System.Linq;
using Raven.Server.Routing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.AI;

public class AiAssistantMigrationRoutes(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Ai)]
    public void The_proxy_exposes_the_six_cdc_planner_routes_as_post()
    {
        var routes = RouteScanner.Scan(attr => attr.Path.StartsWith("/assistant/migration/")).Values;

        Assert.All(routes, route => Assert.Equal("POST", route.Method));
        Assert.Equal(
        [
            "/assistant/migration/area/ask",
            "/assistant/migration/area/start",
            "/assistant/migration/plan/get",
            "/assistant/migration/plan/save",
            "/assistant/migration/proposal/ask",
            "/assistant/migration/proposal/start"
        ], routes.Select(route => route.Path).Order());
    }
}
