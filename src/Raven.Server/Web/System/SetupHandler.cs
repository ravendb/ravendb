using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Server.Commercial;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System
{
    public class SetupHandler : RequestHandler
    {
        [RavenAction("/setup/dns-n-cert", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task DnsCertBridge(string action) // Action can be: claim | user-domain
        {
            AssertOnlyInSetupMode();

            var content = new StreamContent(RequestBodyStream());
            var response = await ApiHttpClient.Instance.PostAsync("/v4/dns-n-cert/" + action, content).ConfigureAwait(false);

            HttpContext.Response.StatusCode = (int)response.StatusCode;
            using (var responseStream = await response.Content.ReadAsStreamAsync())
            {
                await responseStream.CopyToAsync(ResponseBodyStream());
            }
        }

        [RavenAction("/setup/registration-info", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task RegistrationInfo()
        {
            // TODO create /v4/dns-n-cert/registration-info endpoint in api.ravendb.net. It's the same as claim but without the actual claiming.. just checking
            // TODO when endpoint is ready uncomment this and delete fake impl
            /*AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(RequestBodyStream(), "check-domain"))
            {
                var claimDomainInfo = JsonDeserializationServer.ClaimDomainInfo(setupInfoJson);

                var content = new StringContent(JsonConvert.SerializeObject(claimDomainInfo), Encoding.UTF8, "application/json");
                var response = await ApiHttpClient.Instance.PostAsync("/v4/dns-n-cert/registration-info", content).ConfigureAwait(false);

                HttpContext.Response.StatusCode = (int)response.StatusCode;
                var serverResponse = await response.Content.ReadAsStreamAsync();
                await serverResponse.CopyToAsync(ResponseBodyStream());
            }*/

            AssertOnlyInSetupMode();
            
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(RequestBodyStream(), "domain-list"))
            {
                var listDomainsInfo = JsonDeserializationServer.ListDomainsInfo(setupInfoJson);

                /* TODO
                 list existing domains associated with given license 
                 
                 don't return 404 when not found - instead return empty list 
                 
                 this endpoint returns the same output as /setup/claim but it doesn't claim any domain
                 optionally we might merge those 2 endpoint and make claim request optional
                 
                 */

                { //TODO: DELETE ME! - this is temporary fake impl!
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                    
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        WriteFakeClaimResult(writer);
                    }
                }
            }
            return Task.CompletedTask;
        }

        private void WriteFakeClaimResult(BlittableJsonTextWriter writer) //TODO: remove me !
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Domains");
            writer.WriteStartObject();
            
            /* TODO
            writer.WritePropertyName("oren");
            writer.WriteStartArray();
            writer.WriteEndArray();
            writer.WriteComma();
            writer.WritePropertyName("marcin");
            writer.WriteStartArray();
            writer.WriteEndArray();
            */
            
            writer.WriteEndObject();
            writer.WriteComma();
            writer.WritePropertyName("Email");
            writer.WriteString("marcin@ravendb.net");
            writer.WriteEndObject();
        }
        
        [RavenAction("/setup/claim", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task ClaimDomain()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(RequestBodyStream(), "claim-domain"))
            {
                var claimDomainInfo = JsonDeserializationServer.ClaimDomainInfo(setupInfoJson);

                var content = new StringContent(JsonConvert.SerializeObject(claimDomainInfo), Encoding.UTF8, "application/json");
                var response = await ApiHttpClient.Instance.PostAsync("/v4/dns-n-cert/claim", content).ConfigureAwait(false);

                HttpContext.Response.StatusCode = (int)response.StatusCode;
                var serverResponse = await response.Content.ReadAsStreamAsync();
                await serverResponse.CopyToAsync(ResponseBodyStream());
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

        /*[RavenAction("/setup/hosts", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task GetHosts()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var certificateJson = context.ReadForMemory(RequestBodyStream(), "setup-certificate"))
            {
                var certDef = JsonDeserializationServer.CertificateDefinition(certificateJson);

                var certificate = new X509Certificate2(Convert.FromBase64String(certDef.Certificate), certDef.Password);
                var cn = certificate.GetNameInfo(X509NameType.DnsName, false);
                certificate.g
            }
        }*/

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
                jsonObj["Security.UnsecuredAccessAllowed"] = "PublicNetwork";
                jsonObj["ServerUrl"] = setupInfo.ServerUrl;
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
                    progress => SetupManager.SetupSecuredTask(progress, operationCancelToken.Token, setupInfo),
                    operationId.Value, operationCancelToken);
                
                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                var nodeCert = new X509Certificate2(Convert.FromBase64String(setupInfo.NodeSetupInfos[SetupManager.LocalNodeTag].Certificate), setupInfo.NodeSetupInfos[SetupManager.LocalNodeTag].Password);
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
                    progress => SetupManager.SetupLetsEncryptTask(progress, operationCancelToken.Token, setupInfo),
                    operationId.Value, operationCancelToken);

                var zip = ((SetupProgressAndResult)operationResult).SettingsZipFile;

                var contentDisposition = $"attachment; filename={setupInfo.Domain}.Cluster.Settings.zip";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "binary/octet-stream";

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                HttpContext.Response.Body.Write(zip, 0, zip.Length);
            }
        }

        [RavenAction("/setup/validate", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupValidate()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();

            var setupModeString = GetQueryStringValueAndAssertIfSingleAndNotEmpty("setupMode");
            SetupMode setupMode = (SetupMode)Enum.Parse(typeof(SetupMode), setupModeString);

            var operationCancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
            var operationId = GetLongQueryString("operationId", false);

            if (operationId.HasValue == false)
                operationId = ServerStore.Operations.GetNextOperationId();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(stream, "setup-validate"))
            {
                var setupInfo = JsonDeserializationServer.SetupInfo(setupInfoJson);
                
                await ServerStore.Operations.AddOperation(
                    null,
                    "Setting up RavenDB in secured mode.",
                    Documents.Operations.Operations.OperationType.Setup,
                    progress => SetupManager.SetupValidateTask(progress, operationCancelToken.Token, setupInfo, ServerStore, setupMode),
                    operationId.Value, operationCancelToken);
                
            }
        }

        [RavenAction("/setup/generate", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupGenerate()
        {
            AssertOnlyInSetupMode();

            ServerStore.EnsureNotPassive();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var operationId = GetLongQueryString("operationId", false);
                if (operationId.HasValue == false)
                    operationId = ServerStore.Operations.GetNextOperationId();

                var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();

                var certificateJson = ctx.ReadForDisk(stream, "certificate-generation");

                var certificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                byte[] pfx = null;
                
                await ServerStore.Operations.AddOperation(
                        null, "Generate certificate: " + certificate.Name,
                        Documents.Operations.Operations.OperationType.CertificateGeneration,
                        async onProgress =>
                        {
                            pfx = await SetupManager.GenerateCertificateTask(certificate, ServerStore);

                            return ClientCertificateGenerationResult.Instance;
                        },
                        operationId.Value);
                
                var contentDisposition = $"attachment; filename={certificate.Name}.pfx";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "binary/octet-stream";

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                HttpContext.Response.Body.Write(pfx, 0, pfx.Length);
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
                    progress => SetupManager.SetupLetsEncryptTask(progress, operationCancelToken.Token, setupInfo),
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
