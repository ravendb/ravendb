using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public class SetupHandler : RequestHandler
    {
        [RavenAction("/setup/alive", "OPTIONS", AuthorizationStatus.UnauthenticatedClients)]
        public Task AllowPreflightRequest()
        {
            SetupCORSHeaders();
            HttpContext.Response.Headers.Remove("Content-Type");
            return Task.CompletedTask;
        }

        [RavenAction("/setup/alive", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task ServerAlive()
        {
            SetupCORSHeaders();
            return NoContent();
        }

        [RavenAction("/setup/dns-n-cert", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task DnsCertBridge()
        {
            AssertOnlyInSetupMode();
            var action = GetQueryStringValueAndAssertIfSingleAndNotEmpty("action"); // Action can be: claim | user-domains | check-availability

            using (var reader = new StreamReader(RequestBodyStream()))
            {
                var payload = await reader.ReadToEndAsync();
                var content = new StringContent(payload, Encoding.UTF8, "application/json");
                var response = await ApiHttpClient.Instance.PostAsync("/api/v1/dns-n-cert/" + action, content).ConfigureAwait(false);

                HttpContext.Response.StatusCode = (int)response.StatusCode;
                using (var responseStream = await response.Content.ReadAsStreamAsync())
                {
                    await responseStream.CopyToAsync(ResponseBodyStream());
                }
            }
        }

        [RavenAction("/setup/user-domains", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task UserDomains()
        {
            AssertOnlyInSetupMode();

            using (var reader = new StreamReader(RequestBodyStream()))
            {
                var payload = await reader.ReadToEndAsync();
                var content = new StringContent(payload, Encoding.UTF8, "application/json");
                var response = await ApiHttpClient.Instance.PostAsync("/api/v1/dns-n-cert/user-domains", content).ConfigureAwait(false);

                HttpContext.Response.StatusCode = (int)response.StatusCode;

                if (response.IsSuccessStatusCode == false)
                {
                    using (var responseStream = await response.Content.ReadAsStreamAsync())
                    {
                        await responseStream.CopyToAsync(ResponseBodyStream());
                    }
                    return;
                }

                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var responseString = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                    var results = JsonConvert.DeserializeObject<UserDomainsResult>(responseString);

                    var fullResult = new UserDomainsWithIps
                    {
                        Email = results.Email,
                        Domains = new Dictionary<string, List<SubDomainAndIps>>()
                    };

                    foreach (var domain in results.Domains)
                    {
                        var list = new List<SubDomainAndIps>();
                        foreach (var subDomain in domain.Value)
                        {
                            try
                            {
                                list.Add(new SubDomainAndIps
                                {
                                    SubDomain = subDomain,
                                    Ips = Dns.GetHostAddresses(subDomain + "." + SetupManager.RavenDbDomain).Select(ip => ip.ToString()).ToList(),
                                });
                            }
                            catch (Exception)
                            {
                                continue;
                            }
                        }
                        fullResult.Domains.Add(domain.Key, list);
                    }
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        var blittable = EntityToBlittable.ConvertEntityToBlittable(fullResult, DocumentConventions.Default, context);
                        context.Write(writer, blittable);
                    }
                }
            }
        }

        [RavenAction("/setup/parameters", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetSetupParameters()
        {
            AssertOnlyInSetupMode();
            var setupParameters =  SetupParameters.Get(ServerStore);
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(SetupParameters.FixedServerPortNumber));
                
                if (setupParameters.FixedServerPortNumber.HasValue)
                {
                    writer.WriteInteger(setupParameters.FixedServerPortNumber.Value);
                }
                else
                {
                    writer.WriteNull();   
                }
                
                writer.WriteComma();
                
                writer.WritePropertyName(nameof(SetupParameters.IsPosix));
                writer.WriteBool(setupParameters.IsPosix);

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/setup/ips", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetIps()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("MachineName");
                writer.WriteString(Environment.MachineName);
                writer.WriteComma();
                writer.WritePropertyName("NetworkInterfaces");
                writer.WriteStartArray();
                var first = true;
                foreach (var netInterface in NetworkInterface.GetAllNetworkInterfaces())
                {
                    var ips = netInterface.GetIPProperties().UnicastAddresses
                        .Where(x =>
                        {
                            // filter 169.254.xxx.xxx out, they are not meaningful for binding
                            if (x.Address.AddressFamily != AddressFamily.InterNetwork)
                                return false;
                            var addressBytes = x.Address.GetAddressBytes();
                            return addressBytes[0] != 169 || addressBytes[1] != 254;
                        })
                        .Select(addr => addr.Address.ToString())
                        .ToList();


                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteStartObject();
                    writer.WritePropertyName("Name");
                    writer.WriteString(netInterface.Name);
                    writer.WriteComma();
                    writer.WritePropertyName("Description");
                    writer.WriteString(netInterface.Description);
                    writer.WriteComma();
                    writer.WriteArray("Addresses", ips);
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }
            return Task.CompletedTask;
        }

        [RavenAction("/setup/hosts", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetHosts()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var certificateJson = context.ReadForMemory(RequestBodyStream(), "setup-certificate"))
            {
                var certDef = JsonDeserializationServer.CertificateDefinition(certificateJson);

                X509Certificate2 certificate = null;
                string cn;

                try
                {
                    certificate = certDef.Password == null
                        ? new X509Certificate2(Convert.FromBase64String(certDef.Certificate))
                        : new X509Certificate2(Convert.FromBase64String(certDef.Certificate), certDef.Password);

                    cn = certificate.GetNameInfo(X509NameType.DnsName, false);
                }
                catch (Exception e)
                {
                    throw new BadRequestException($"Failed to extract CN and SAN from certificate {certificate?.FriendlyName}. Maybe password is wrong?", e);
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("CN");
                    writer.WriteString(cn);
                    writer.WriteComma();
                    writer.WritePropertyName("AlternativeNames");
                    writer.WriteStartArray();

                    var first = true;
                    foreach (var value in SetupManager.GetCertificateAlternativeNames(certificate))
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        writer.WriteString(value);
                    }
                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/setup/unsecured", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task SetupUnsecured()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(RequestBodyStream(), "setup-unsecured"))
            {
                // Making sure we don't have leftovers from previous setup
                try
                {
                    using (var tx = context.OpenWriteTransaction())
                    {
                        ServerStore.Engine.DeleteTopology(context);
                        tx.Commit();
                    }
                }
                catch (Exception)
                {
                    // ignored
                }

                var setupInfo = JsonDeserializationServer.UnsecuredSetupInfo(setupInfoJson);

                var settingsJson = File.ReadAllText(ServerStore.Configuration.ConfigPath);

                dynamic jsonObj = JsonConvert.DeserializeObject(settingsJson);

                jsonObj[RavenConfiguration.GetKey(x => x.Core.SetupMode)] = nameof(SetupMode.Unsecured);
                jsonObj[RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)] = nameof(UnsecuredAccessAddressRange.PublicNetwork); // TODO handle server side.
                jsonObj[RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = string.Join(";", setupInfo.Addresses.Select(ip => IpAddressToUrl(ip, setupInfo.Port)));

                var json = JsonConvert.SerializeObject(jsonObj, Formatting.Indented);

                SetupManager.WriteSettingsJsonLocally(ServerStore.Configuration.ConfigPath, json);
            }

            return NoContent();
        }

        [RavenAction("/setup/secured", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupSecured()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();
            var operationCancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(stream, "setup-secured"))
            {
                var setupInfo = JsonDeserializationServer.SetupInfo(setupInfoJson);

                var operationResult = await ServerStore.Operations.AddOperation(
                    null,
                    "Setting up RavenDB in secured mode.",
                    Documents.Operations.Operations.OperationType.Setup,
                    progress => SetupManager.SetupSecuredTask(progress, operationCancelToken.Token, setupInfo, ServerStore),
                    operationId.Value, operationCancelToken);

                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                var nodeCert = setupInfo.Password == null
                    ? new X509Certificate2(Convert.FromBase64String(setupInfo.Certificate))
                    : new X509Certificate2(Convert.FromBase64String(setupInfo.Certificate), setupInfo.Password);

                var cn = nodeCert.GetNameInfo(X509NameType.DnsName, false);

                var contentDisposition = $"attachment; filename={cn}.Cluster.Settings.zip";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "application/octet-stream";

                HttpContext.Response.Body.Write(zip, 0, zip.Length);
            }
        }

        [RavenAction("/setup/letsencrypt/agreement", "GET", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupAgreement()
        {
            AssertOnlyInSetupMode();

            var email = GetQueryStringValueAndAssertIfSingleAndNotEmpty("email");

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var baseUri = new Uri("https://letsencrypt.org/");
                var uri = new Uri(baseUri, await SetupManager.LetsEncryptAgreement(email, ServerStore));

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Uri");
                    writer.WriteString(uri.AbsoluteUri);
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/setup/letsencrypt", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupLetsEncrypt()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();

            var operationCancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(stream, "setup-lets-encrypt"))
            {
                var setupInfo = JsonDeserializationServer.SetupInfo(setupInfoJson);

                var operationResult = await ServerStore.Operations.AddOperation(
                    null, "Setting up RavenDB with a Let's Encrypt certificate",
                    Documents.Operations.Operations.OperationType.Setup,
                    progress => SetupManager.SetupLetsEncryptTask(progress, operationCancelToken.Token, setupInfo, ServerStore),
                    operationId.Value, operationCancelToken);

                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                var contentDisposition = $"attachment; filename={setupInfo.Domain}.Cluster.Settings.zip";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "application/octet-stream";

                HttpContext.Response.Body.Write(zip, 0, zip.Length);
            }
        }

        [RavenAction("/setup/finish", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task SetupFinish()
        {
            AssertOnlyInSetupMode();

            Task.Run(async () =>
            {
                // we want to give the studio enough time to actually
                // get a valid response from the server before we reset
                await Task.Delay(250);

                Program.ResetServerMre.Set();
                Program.ShutdownServerMre.Set();
            });

            return NoContent();
        }

        private void AssertOnlyInSetupMode()
        {
            if (ServerStore.Configuration.Core.SetupMode == SetupMode.Initial)
                return;

            throw new UnauthorizedAccessException("RavenDB has already been setup. Cannot use the /setup endpoints any longer.");
        }

        private static string IpAddressToUrl(string address, int port)
        {
            var url = "http://" + address;
            if (port != 80)
                url += ":" + port;
            return url;
        }
    }
}
