using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.NetworkInformation;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Commercial;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Utils;
using Raven.Server.Web.Authentication;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Platform;

namespace Raven.Server.Web.System
{
    public class SetupHandler : RequestHandler
    {
        [RavenAction("/setup/claim", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task ClaimDomain()
        {
            AssertOnlyInSetupMode();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(RequestBodyStream(), "claim-domain"))
            {
                var claimDomainInfo = JsonDeserializationServer.ClaimDomainInfo(setupInfoJson);

                var response = await ApiHttpClient.Instance.PostAsync("/api/v4/dns-n-cert/claim",
                        new StringContent(JsonConvert.SerializeObject(claimDomainInfo), Encoding.UTF8, "application/json"))
                    .ConfigureAwait(false);

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

        [RavenAction("/setup/unsecured", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task SetupUnsecured()
        {
            AssertOnlyInSetupMode();
            // also get public server url and setup server, make sure we can access it, etc.
            // validate by getting a GUID from the temp server.

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(RequestBodyStream(), "setup-unsecured"))
            {
                var setupInfo = JsonDeserializationServer.UnsecuredSetupInfo(setupInfoJson);
                // also set public server url
                SetupManager.WriteSettingsJsonFile(null, null, setupInfo.ServerUrl, SetupManager.SettingsFileName, SetupMode.Unsecured, true);
            }

            return NoContent();
        }

        [RavenAction("/setup/secured", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task SetupSecured()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(stream, "setup-secured"))
            {

                var setupInfo = JsonDeserializationServer.SecuredSetupInfo(setupInfoJson);

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    if (string.IsNullOrWhiteSpace(node.Certificate))
                        throw new ArgumentException($"{nameof(node.Certificate)} is a mandatory property for a secured setup");
                    if (string.IsNullOrWhiteSpace(node.ServerUrl))
                        throw new ArgumentException($"{nameof(node.ServerUrl)} is a mandatory property for a secured setup");
                    if (string.IsNullOrWhiteSpace(node.NodeTag))
                        throw new ArgumentException($"{nameof(node.NodeTag)} is a mandatory property for a secured setup");

                    if (PlatformDetails.RunningOnPosix)
                    {
                        AdminCertificatesHandler.ValidateCaExistsInOsStores(node.Certificate, "Setup server certificate", ServerStore);
                    }
                }

                // Prepare settings.json files, and write the local one to disk
                var settingsPath = SetupManager.SettingsFileName;
                var jsons = new Dictionary<string, string>();
                SecuredSetupInfo.NodeInfo localNode = null;

                foreach (var node in setupInfo.NodeSetupInfos)
                {
                    try
                    {
                        if (node.NodeTag == "A")
                        {
                            jsons.Add(node.NodeTag,
                                SetupManager.WriteSettingsJsonFile(node.Certificate, node.PublicServerUrl, node.ServerUrl, settingsPath, SetupMode.Secured, modifyLocalServer: true));
                            localNode = node;
                        }
                        else
                            jsons.Add(node.NodeTag, SetupManager.WriteSettingsJsonFile(node.Certificate, node.PublicServerUrl, node.ServerUrl, settingsPath, SetupMode.Secured, modifyLocalServer: false));
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to update {settingsPath} with new configuration.", e);
                    }
                }

                // Here we need to take the jsons, create files and zip them all together
                // Need to make this an async operation, just like when sending a certificate for the studio... to know that we're done and sending a file
                var zip = new byte[]{};
                
                try
                {
                    var ips = localNode?.Ips.Select(ip => new IPEndPoint(IPAddress.Parse(ip), localNode.Port)).ToArray();

                    var certBytes = Convert.FromBase64String(localNode?.Certificate);
                    var x509Certificate2 = new X509Certificate2(certBytes);
                    ServerStore.SetupManager.AssertServerCanStartSecured(x509Certificate2, localNode?.ServerUrl, ips, settingsPath);

                    // Load the certificate in the local server, so we can generate client certs later
                    Server.ClusterCertificateHolder = SecretProtection.ValidateCertificateAndCreateCertificateHolder(localNode?.Certificate, "Setup", x509Certificate2, certBytes);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to start server with the new configuration {settingsPath}.", e);

                }

                var contentDisposition = "attachment; filename=settings.zip";
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
                var uri = await ServerStore.SetupManager.LetsEncryptAgreement(email);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Uri");
                    writer.WriteString(uri.AbsolutePath);
                    writer.WriteEndObject();
                }
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
        }

        [RavenAction("/setup/letsencrypt", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public Task SetupLetsEncrypt()
        {
            AssertOnlyInSetupMode();

            var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();


            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var setupInfoJson = context.ReadForMemory(stream, "setup-lets-encrypt"))
            {
                var setupInfo = JsonDeserializationServer.SecuredSetupInfo(setupInfoJson);

                var operationCancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
                var operationId = ServerStore.Operations.GetNextOperationId();

                ServerStore.Operations.AddOperation(
                    null,
                    "Setting up RavenDB with a Let's Encrypt certificate",
                    Documents.Operations.Operations.OperationType.SetupLetsEncrypt,
                    progress => ServerStore.SetupManager.FetchCertificateTask(progress, operationCancelToken.Token, setupInfo),
                    operationId, operationCancelToken);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteOperationId(context, operationId);
                }

                // Here we need to take the jsons, create files and zip them all together
                // Need to make this an async operation, just like when sending a certificate for the studio... to know that we're done and sending a file
                var zip = new byte[]{};

                var contentDisposition = "attachment; filename=settings.zip";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "binary/octet-stream";

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                HttpContext.Response.Body.Write(zip, 0, zip.Length);
                return Task.CompletedTask;
            }
        }

        [RavenAction("/setup/generate", "POST", AuthorizationStatus.UnauthenticatedClients)]
        public async Task Generate()
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
                await
                    ServerStore.Operations.AddOperation(
                        null,
                        "Generate certificate: " + certificate.Name,
                        Documents.Operations.Operations.OperationType.CertificateGeneration,
                        async onProgress =>
                        {
                            pfx = await GenerateCertificateInternal(certificate);

                            return ClientCertificateGenerationResult.Instance;
                        },
                        operationId.Value);

                var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(certificate.Name) + ".pfx";
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
                var setupInfo = JsonDeserializationServer.SecuredSetupInfo(setupInfoJson);

                var operationCancelToken = new OperationCancelToken(ServerStore.ServerShutdown);
                var operationId = ServerStore.Operations.GetNextOperationId();

                ServerStore.Operations.AddOperation(
                    null,
                    "Setting up RavenDB with a Let's Encrypt certificate",
                    Documents.Operations.Operations.OperationType.SetupLetsEncrypt,
                    progress => ServerStore.SetupManager.FetchCertificateTask(progress, operationCancelToken.Token, setupInfo),
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
        
        // Duplicate of AdminCertificatesHandler.GenerateCertificateInternal, but used by an unauthenticated client during setup only
        private async Task<byte[]> GenerateCertificateInternal(CertificateDefinition certificate)
        {
            if (string.IsNullOrWhiteSpace(certificate.Name))
                throw new ArgumentException($"{nameof(certificate.Name)} is a required field in the certificate definition");

            if (Server.ClusterCertificateHolder?.Certificate == null)
                throw new InvalidOperationException($"Cannot generate the client certificate '{certificate.Name}' becuase the server certificate is not loaded.");

            if (PlatformDetails.RunningOnPosix)
            {
                AdminCertificatesHandler.ValidateCaExistsInOsStores(certificate.Certificate, certificate.Name, ServerStore);
            }

            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(certificate.Name, Server.ClusterCertificateHolder);

            var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + selfSignedCertificate.Thumbprint,
                new CertificateDefinition
                {
                    Name = certificate.Name,
                    // this does not include the private key, that is only for the client
                    Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                    Permissions = certificate.Permissions,
                    SecurityClearance = certificate.SecurityClearance,
                    Thumbprint = selfSignedCertificate.Thumbprint
                }));
            await ServerStore.Cluster.WaitForIndexNotification(res.Index);

            return selfSignedCertificate.Export(X509ContentType.Pfx, certificate.Password);
        }
    }
}
