using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Raven.Quill.Contracts;
using Raven.Quill.Hosting;

namespace Raven.Quill.Endpoints;

public static class BootstrapEndpoints
{
    public static void Map(WebApplication app)
    {
        var group = app.MapGroup("/api/bootstrap");
        group.MapGet("/status", GetStatus)
            .WithName("bootstrap.status")
            .Produces<BootstrapStatusResponse>();
    }

    private static IResult GetStatus(IBootstrapState state) =>
        Results.Ok(new BootstrapStatusResponse(state.Phase, state.Reason));
}
