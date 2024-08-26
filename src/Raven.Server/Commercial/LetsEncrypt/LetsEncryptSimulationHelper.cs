using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Raven.Client;
using Raven.Client.Http;
using Raven.Server.Config;
using Raven.Server.Https;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow.Platform;

namespace Raven.Server.Commercial.LetsEncrypt;

public sealed class LetsEncryptSimulationHelper
{
    internal static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<LetsEncryptSimulationHelper>();

    internal sealed class UniqueResponseResponder : IStartup
    {
        private readonly string _response;

        public UniqueResponseResponder(string response)
        {
            _response = response;
        }

        public IServiceProvider ConfigureServices(IServiceCollection services)
        {
            return services.BuildServiceProvider();
        }

        public void Configure(IApplicationBuilder app)
        {
            app.Run(async context =>
            {
                await context.Response.WriteAsync(_response);
            });
        }
    }
    public static async Task SimulateRunningServer(ServerStore serverStore, X509Certificate2 serverCertificate, string serverUrl, string nodeTag,
       IPEndPoint[] addresses, int port, string settingsPath, SetupMode setupMode, CancellationToken token)
    {
        var configuration = RavenConfiguration.CreateForServer(null, settingsPath);
        configuration.Initialize();
        var guid = Guid.NewGuid().ToString();

        IWebHost webHost = null;
        try
        {
            try
            {
                var responder = new UniqueResponseResponder(guid);

                var webHostBuilder = new WebHostBuilder()
                    .CaptureStartupErrors(captureStartupErrors: true)
                    .UseKestrel(options =>
                    {
                        var httpsConnectionMiddleware = new HttpsConnectionMiddleware(serverStore.Server, options, serverCertificate);

                        if (addresses.Length == 0)
                        {
                            var defaultIp = new IPEndPoint(IPAddress.Parse(Constants.Network.AnyIp), port == 0 ? Constants.Network.DefaultSecuredRavenDbHttpPort : port);

                            options.Listen(defaultIp, listenOptions =>
                            {
                                listenOptions
                                    .UseHttps()
                                    .Use(httpsConnectionMiddleware.OnConnectionAsync);
                            });
                            if (Logger.IsInfoEnabled)
                                Logger.Info($"List of ip addresses for node '{nodeTag}' is empty. WebHost listening to {defaultIp}");
                        }

                        foreach (var address in addresses)
                        {
                            options.Listen(address, listenOptions =>
                            {
                                listenOptions
                                    .UseHttps()
                                    .Use(httpsConnectionMiddleware.OnConnectionAsync);
                            });
                        }
                    })
                    .UseSetting(WebHostDefaults.ApplicationKey, "Setup simulation")
                    .ConfigureServices(collection => { collection.AddSingleton(typeof(IStartup), responder); })
                    .UseShutdownTimeout(TimeSpan.FromMilliseconds(150));

                webHost = webHostBuilder.Build();

                await webHost.StartAsync(token);
            }
            catch (Exception e)
            {
                string linuxMsg = null;
                if (PlatformDetails.RunningOnPosix && (port == 80 || port == 443))
                {
                    linuxMsg = $"It can happen if port '{port}' is not allowed for the non-root RavenDB process." +
                               $"Try using setcap to allow it: sudo setcap CAP_NET_BIND_SERVICE=+eip {Path.Combine(AppContext.BaseDirectory, "Raven.Server")}";
                }

                var also = linuxMsg == null ? string.Empty : "also";
                var externalIpMsg = setupMode == SetupMode.LetsEncrypt
                    ? $"It can {also} happen if the ip is external (behind a firewall, docker). If this is the case, try going back to the previous screen and add the same ip as an external ip."
                    : string.Empty;

                throw new InvalidOperationException(
                    $"Failed to start WebHost on node '{nodeTag}'. The specified ip address might not be reachable due to network issues. {linuxMsg}{Environment.NewLine}{externalIpMsg}{Environment.NewLine}" +
                    $"Settings file:{settingsPath}.{Environment.NewLine}" +
                    $"IP addresses: {string.Join(", ", addresses.AsEnumerable().Select(address => address.ToString()))}.", e);
            }

            using (var httpMessageHandler = new HttpClientHandler())
            {
                // on MacOS this is not supported because Apple...
                if (PlatformDetails.RunningOnMacOsx == false)
                {
                    httpMessageHandler.ServerCertificateCustomValidationCallback += (_, certificate2, _, _) =>
                    // we want to verify that we get the same thing back
                    {
                        if (certificate2.Thumbprint != serverCertificate.Thumbprint)
                            throw new InvalidOperationException("Expected to get " + serverCertificate.FriendlyName + " with thumbprint " +
                                                                serverCertificate.Thumbprint + " but got " +
                                                                certificate2.FriendlyName + " with thumbprint " + certificate2.Thumbprint);
                        return true;
                    };
                }

                using (var client = new RavenHttpClient(httpMessageHandler) { BaseAddress = new Uri(serverUrl) })
                {
                    HttpResponseMessage response = null;
                    string result = null;
                    try
                    {
                        using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(30)))
                        using (var cts = CancellationTokenSource.CreateLinkedTokenSource(token, cancellationTokenSource.Token))
                        {
                            response = await client.GetAsync("/are-you-there?", cts.Token);
                            response.EnsureSuccessStatusCode();
                            result = await response.Content.ReadAsStringWithZstdSupportAsync(cts.Token);
                            if (result != guid)
                            {
                                throw new InvalidOperationException($"Expected result guid: {guid} but got {result}.");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (setupMode == SetupMode.Secured && await RavenDnsRecordHelper.CanResolveHostNameLocally(serverUrl, addresses) == false)
                        {
                            throw new InvalidOperationException(
                                $"Failed to resolve '{serverUrl}'. Try to clear your local/network DNS cache and restart validation.", e);
                        }

                        throw new InvalidOperationException($"Client failed to contact WebHost listening to '{serverUrl}'.{Environment.NewLine}" +
                                                            $"Are you blocked by a firewall? Make sure the port is open.{Environment.NewLine}" +
                                                            $"Settings file:{settingsPath}.{Environment.NewLine}" +
                                                            $"IP addresses: {string.Join(", ", addresses.AsEnumerable().Select(address => address.ToString()))}.{Environment.NewLine}" +
                                                            $"Response: {response?.StatusCode}.{Environment.NewLine}{result}", e);
                    }
                }
            }
        }
        finally
        {
            if (webHost != null)
                await webHost.StopAsync(TimeSpan.Zero);
        }
    }
}
