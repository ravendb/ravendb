using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Web.System
{
    public class SetupHandler : RequestHandler
    {
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
                                    Ips = Dns.GetHostAddresses($"{subDomain}.{domain.Key}.{SetupManager.RavenDbDomain}").Select(ip => ip.ToString()).ToList(),
                                });
                            }
                            catch (Exception e)
                            {
                                continue;
                                // need to log this?
                                throw new InvalidOperationException($"Failed to query the ips for host {subDomain}.{domain.Key}.{SetupManager.RavenDbDomain}", e);
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
                    var ips = netInterface.GetIPProperties().UnicastAddresses.Select(addr => addr.Address.ToString()).ToList();
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

                X509Extension sanNames;
                X509Certificate2 certificate = null;
                string cn;

                try
                {
                    certificate = certDef.Password == null
                        ? new X509Certificate2(Convert.FromBase64String(certDef.Certificate))
                        : new X509Certificate2(Convert.FromBase64String(certDef.Certificate), certDef.Password);

                    cn = certificate.GetNameInfo(X509NameType.DnsName, false);
                    sanNames = certificate.Extensions["2.5.29.17"]; // Alternative names
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

                    if (sanNames != null)
                    {
                        writer.WriteComma();
                        writer.WritePropertyName("AlternativeNames");
                        writer.WriteStartArray();

                        var first = true;
                        foreach (var line in sanNames.Format(true).Split(new[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries))
                        {
                            string[] parts;

                            if (line.Contains('='))
                            {
                                parts = line.Split('=');
                            } 
                            else if (line.Contains(':'))
                            {
                                parts = line.Split(':');
                            }
                            else
                            {
                                throw new InvalidOperationException($"Could not parse SAN names: {line}");
                            }

                            var value = parts.Length > 0 ? parts[1] : null;

                            if (first == false)
                                writer.WriteComma();
                            first = false;

                            writer.WriteString(value);
                        }
                        writer.WriteEndArray();
                    }

                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/setup/unsecured", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task SetupUnsecured()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(RequestBodyStream(), "setup-unsecured"))
            {
                var setupInfo = JsonDeserializationServer.UnsecuredSetupInfo(setupInfoJson);

                var settingsJson = File.ReadAllText(SetupManager.SettingsPath);

                dynamic jsonObj = JsonConvert.DeserializeObject(settingsJson);
                jsonObj["Setup.Mode"] = SetupMode.Unsecured.ToString();
                jsonObj["Security.UnsecuredAccessAllowed"] = "PublicNetwork"; // TODO handle server side.
                jsonObj["ServerUrl"] = string.Join(";", setupInfo.Addresses.Select(ip => "http://" + ip + ":" + setupInfo.Port));
                jsonObj.Remove("PublicServerUrl");
                if (string.IsNullOrEmpty(setupInfo.PublicServerUrl) == false)
                {
                    jsonObj["PublicServerUrl"] = setupInfo.PublicServerUrl;                    
                }
                jsonObj["Security.Certificate.Base64"] = null;
                var json = JsonConvert.SerializeObject(jsonObj, Formatting.Indented);

                SetupManager.WriteSettingsJsonLocally(SetupManager.SettingsPath, json);
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
                HttpContext.Response.ContentType = "binary/octet-stream";

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
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
                var uri = new Uri(baseUri, await SetupManager.LetsEncryptAgreement(email));

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
                HttpContext.Response.ContentType = "binary/octet-stream";

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
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

        [RavenAction("/admin/setup/letsencrypt/force-renew", "POST", AuthorizationStatus.ClusterAdmin)]
        public Task ForceRenew()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForDisk(RequestBodyStream(), "setup-lets-encrypt"))
            {
                var setupInfo = JsonDeserializationServer.SetupInfo(setupInfoJson);

                var operationCancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
                var operationId = ServerStore.Operations.GetNextOperationId();

                ServerStore.Operations.AddOperation(
                    null,
                    "Setting up RavenDB with a Let's Encrypt certificate",
                    Documents.Operations.Operations.OperationType.Setup,
                    progress => SetupManager.SetupLetsEncryptTask(progress, operationCancelToken.Token, setupInfo, ServerStore),
                    operationId, operationCancelToken);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationId(context, operationId);
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                return Task.CompletedTask;
            }
        }

        private void AssertOnlyInSetupMode()
        {
            if (ServerStore.Configuration.Core.SetupMode == SetupMode.Initial)
                return;

            throw new UnauthorizedAccessException("RavenDB has already been setup. Cannot use the /setup endpoints any longer.");
        }
    }
}
