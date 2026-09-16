using System.Net.Sockets;
using Raven.Quill.Contracts;
using Raven.Quill.Infrastructure;

namespace Raven.Quill.Endpoints;

public static class DnsEndpoints
{
    public static void Map(WebApplication app)
    {
        app.MapGet("/api/dns/ip-binding", GetIpBindingAsync)
            .WithTags("dns")
            .WithName("dns.ipBinding")
            .RequireAuthorization()
            .Produces<IpBindingResponse>()
            .Produces<ApiErrorResponse>(StatusCodes.Status400BadRequest)
            .Produces<ApiErrorResponse>(StatusCodes.Status502BadGateway);
    }

    private static async Task<IResult> GetIpBindingAsync(HttpContext context, IDnsResolver resolver, CancellationToken token)
    {
        var hostname = context.Request.Host.Host;
        if (Uri.CheckHostName(hostname) != UriHostNameType.Dns)
            return Results.BadRequest(new ApiErrorResponse($"'{hostname}' is not a DNS name"));

        try
        {
            var addresses = await resolver.ResolveIPv4Async(hostname, token);
            return Results.Ok(new IpBindingResponse(hostname, addresses));
        }
        catch (SocketException)
        {
            return Results.Json(
                new ApiErrorResponse($"DNS lookup for '{hostname}' failed"),
                statusCode: StatusCodes.Status502BadGateway);
        }
    }
}
