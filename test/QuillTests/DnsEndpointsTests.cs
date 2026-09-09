using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using QuillTests.E2E.Fixtures;
using Raven.Quill.Auth;
using Raven.Quill.Contracts;
using Raven.Quill.Infrastructure;
using Tests.Infrastructure;
using Xunit;

namespace QuillTests;

public class DnsEndpointsTests(ITestOutputHelper output) : QuillTestBase(output)
{
    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ip_binding_resolves_the_request_host_through_the_registered_resolver()
    {
        var resolver = new FakeDnsResolver { Handler = _ => ["51.210.14.7", "51.210.14.8"] };
        await using var host = await NewHostAsync(configureServices: services =>
        {
            services.RemoveAll<IDnsResolver>();
            services.AddSingleton<IDnsResolver>(resolver);
        });

        var resp = await host.Client.GetAsync(QuillRoutes.DnsIpBinding);
        Assert.Equal(HttpStatusCode.OK, resp.StatusCode);

        var binding = await resp.Content.ReadFromJsonAsync<IpBindingResponse>();
        Assert.NotNull(binding);
        Assert.Equal("localhost", binding.Hostname);
        Assert.Equal(["51.210.14.7", "51.210.14.8"], binding.Addresses);
        Assert.Equal("localhost", resolver.Hostname);

        resolver.Handler = _ => [];
        binding = await host.Client.GetFromJsonAsync<IpBindingResponse>(QuillRoutes.DnsIpBinding);
        Assert.NotNull(binding);
        Assert.Empty(binding.Addresses);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task A_failing_lookup_is_502()
    {
        await using var host = await NewHostAsync(configureServices: services =>
        {
            services.RemoveAll<IDnsResolver>();
            services.AddSingleton<IDnsResolver>(new FakeDnsResolver
            {
                Handler = _ => throw new SocketException((int)SocketError.TryAgain)
            });
        });

        var resp = await host.Client.GetAsync(QuillRoutes.DnsIpBinding);
        Assert.Equal(HttpStatusCode.BadGateway, resp.StatusCode);

        var error = await resp.Content.ReadFromJsonAsync<ApiErrorResponse>();
        Assert.NotNull(error);
        Assert.Contains("DNS lookup", error.Error);
    }

    [RavenFact(RavenTestCategory.Quill)]
    public async Task Ip_binding_without_credential_is_401()
    {
        using var client = Host.Factory.CreateClient();
        client.DefaultRequestHeaders.Remove(ApiKeyAuthenticationHandler.HeaderName);

        var resp = await client.GetAsync(QuillRoutes.DnsIpBinding);
        Assert.Equal(HttpStatusCode.Unauthorized, resp.StatusCode);
    }

    private sealed class FakeDnsResolver : IDnsResolver
    {
        public required Func<string, string[]> Handler { get; set; }
        public string? Hostname { get; private set; }

        public Task<string[]> ResolveIPv4Async(string hostname, CancellationToken token)
        {
            Hostname = hostname;
            return Task.FromResult(Handler(hostname));
        }
    }
}
