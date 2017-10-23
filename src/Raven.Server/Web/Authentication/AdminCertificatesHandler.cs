using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Platform;

namespace Raven.Server.Web.Authentication
{
    public class AdminCertificatesHandler : RequestHandler
    {

        [RavenAction("/admin/certificates", "POST", AuthorizationStatus.Operator)]
        public async Task Generate()
        {
            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node
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
                            pfx = await GenerateCertificateInternal(ctx, certificate);

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

        private async Task<byte[]> GenerateCertificateInternal(TransactionOperationContext ctx, CertificateDefinition certificate)
        {
            ValidateCertificate(certificate, ServerStore);

            if (certificate.SecurityClearance == SecurityClearance.ClusterAdmin && IsClusterAdmin() == false)
            {
                var clientCert = (HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection)?.Certificate;
                var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + clientCert?.Thumbprint);
                throw new InvalidOperationException($"Cannot generate the certificate '{certificate.Name}' with 'Cluster Admin' security clearance because the current client certificate being used has a lower clearance: {clientCertDef.SecurityClearance}");
            }
            
            if (Server.ClusterCertificateHolder?.Certificate == null)
            {
                var keys = new[]
                {
                    RavenConfiguration.GetKey(x => x.Security.CertificatePath),
                    RavenConfiguration.GetKey(x => x.Security.CertificateExec),
                    RavenConfiguration.GetKey(x => x.Security.ClusterCertificatePath),
                    RavenConfiguration.GetKey(x => x.Security.ClusterCertificateExec)
                };

                throw new InvalidOperationException($"Cannot generate the client certificate '{certificate.Name}' becuase the server certificate is not loaded. " +
                                                    $"You can supply a server certificate by using the following configuration keys: {string.Join(", ", keys)}" +
                                                    "For a more detailed explanation please read about authentication and certificates in the RavenDB documentation.");
            }

            if (PlatformDetails.RunningOnPosix)
            {
                ValidateCaExistsInOsStores(certificate);
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

        private void ValidateCaExistsInOsStores(CertificateDefinition certificate)
        {
            // Implementation of SslStream AuthenticateAsServer is different in Linux. See RavenDB-8524
            // A list of allowed CAs is sent from the server to the client. The handshake will fail if the client's CA is not in that list. This list is taken from the root and certificate authority stores of the OS.
            // In this workaround we make sure that the CA (who signed the certificate) is registered in one of the OS stores.

            var x509Certificate2 = new X509Certificate2(Convert.FromBase64String(certificate.Certificate));

            var chain = new X509Chain
            {
                ChainPolicy =
                    {
                        RevocationMode = X509RevocationMode.NoCheck,
                        RevocationFlag = X509RevocationFlag.ExcludeRoot,
                        VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority,
                        VerificationTime = DateTime.UtcNow,
                        UrlRetrievalTimeout = new TimeSpan(0, 0, 0)
                    }
            };

            if (chain.Build(x509Certificate2) == false)
            {
                var status = new StringBuilder();
                if (chain.ChainStatus.Length != 0)
                {
                    status.Append("Chain Status:\r\n");
                    foreach (var chainStatus in chain.ChainStatus)
                        status.Append(chainStatus.Status + " : " + chainStatus.StatusInformation + "\r\n");
                }

                throw new InvalidOperationException($"The certificate chain for {certificate.Name} is broken, admin assistance required. {status}");
            }

            var rootCert = GetRootCertificate(chain);
            if (rootCert == null)
                throw new InvalidOperationException($"The certificate chain for {certificate.Name} is broken. Reason: partial chain, cannot extract CA from chain. Admin assistance required.");


            using (var machineRootStore = new X509Store(StoreName.Root, StoreLocation.LocalMachine, OpenFlags.ReadOnly))
            using (var machineCaStore = new X509Store(StoreName.CertificateAuthority, StoreLocation.LocalMachine, OpenFlags.ReadOnly))
            using (var userRootStore = new X509Store(StoreName.Root, StoreLocation.CurrentUser, OpenFlags.ReadOnly))
            using (var userCaStore = new X509Store(StoreName.CertificateAuthority, StoreLocation.CurrentUser, OpenFlags.ReadOnly))
            {
                // workaround for lack of cert store inheritance RavenDB-8904
                if (machineCaStore.Certificates.Contains(rootCert) == false
                    && machineRootStore.Certificates.Contains(rootCert) == false
                    && userCaStore.Certificates.Contains(rootCert) == false
                    && userRootStore.Certificates.Contains(rootCert) == false)
                {
                    var path = new[]
                    {
                        ServerStore.Configuration.Security.CertificatePath,
                        ServerStore.Configuration.Security.ClusterCertificatePath,
                        ServerStore.Configuration.Security.CertificateExec,
                        ServerStore.Configuration.Security.ClusterCertificateExec
                    }.FirstOrDefault(File.Exists) ?? "no path defined";

                    throw new InvalidOperationException($"Cannot save the client certificate '{certificate.Name}'. " +
                                                        $"First, you must register the CA of the certificate '{rootCert.SubjectName.Name}' in the trusted root store, on the server machine." +
                                                        $"The server certificate is located in: '{path}'" +
                                                        "This step is required because you are trying to save a certificate which was signed by an unknown or self-signed certificate authority.");
                }
            }
        }

        private static X509Certificate2 GetRootCertificate(X509Chain chain)
        {
            if (chain.ChainElements.Count < 1)
                return null;

            var lastElement = chain.ChainElements[chain.ChainElements.Count - 1];

            foreach (var status in lastElement.ChainElementStatus)
            {
                if (status.Status == X509ChainStatusFlags.PartialChain)
                {
                    return null;
                }
            }

            return lastElement.Certificate;
        }

        [RavenAction("/admin/certificates", "PUT", AuthorizationStatus.Operator)]
        public async Task Put()
        {
            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node

            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var certificateJson = ctx.ReadForDisk(RequestBodyStream(), "put-certificate"))
            {
                var certificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                ValidateCertificate(certificate, ServerStore);

                if ((certificate.SecurityClearance == SecurityClearance.ClusterAdmin || certificate.SecurityClearance == SecurityClearance.ClusterNode) && IsClusterAdmin() == false)
                {
                    var clientCert = (HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection)?.Certificate;
                    var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + clientCert?.Thumbprint);
                    throw new InvalidOperationException($"Cannot save the certificate '{certificate.Name}' with '{certificate.SecurityClearance}' security clearance because the current client certificate being used has a lower clearance: {clientCertDef.SecurityClearance}");
                }

                if (string.IsNullOrWhiteSpace(certificate.Certificate))
                    throw new ArgumentException($"{nameof(certificate.Certificate)} is a mandatory property when saving an existing certificate");

                byte[] certBytes;
                try
                {
                    certBytes = Convert.FromBase64String(certificate.Certificate);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {nameof(certificate.Certificate)} property, expected a Base64 value", e);
                }

                var collection = new X509Certificate2Collection();
                collection.Import(certBytes);

                var first = true;
                var collectionPrimaryKey = string.Empty;
                
                foreach (var x509Certificate in collection)
                {
                    var currentCertificate = new CertificateDefinition
                    {
                        Name = certificate.Name,
                        Permissions = certificate.Permissions,
                        SecurityClearance = certificate.SecurityClearance,
                        Password = certificate.Password
                    };
                    
                    if (x509Certificate.HasPrivateKey)
                    {
                        // avoid storing the private key
                        currentCertificate.Certificate = Convert.ToBase64String(x509Certificate.Export(X509ContentType.Cert));
                    }

                    // In case of a collection, we group all the certificates together and treat them as one unit. 
                    // They all have the same name and permissions but a different thumbprint.
                    // The first certificate in the collection will be the primary certificate and its thumbprint will be the one shown in a GET request
                    // The other certificates are secondary certificates and will contain a link to the primary certificate.
                    currentCertificate.Thumbprint = x509Certificate.Thumbprint;
                    currentCertificate.Certificate = Convert.ToBase64String(x509Certificate.Export(X509ContentType.Cert));

                    if (PlatformDetails.RunningOnPosix)
                    {
                        ValidateCaExistsInOsStores(currentCertificate);
                    }

                    if (first)
                    {
                        var firstKey = Constants.Certificates.Prefix + x509Certificate.Thumbprint;
                        collectionPrimaryKey = firstKey;

                        foreach (var cert in collection)
                        {
                            if (Constants.Certificates.Prefix + cert.Thumbprint != firstKey)
                                currentCertificate.CollectionSecondaryKeys.Add(Constants.Certificates.Prefix + cert.Thumbprint);
                        }
                    }
                    else
                        currentCertificate.CollectionPrimaryKey = collectionPrimaryKey;
                    
                    var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + x509Certificate.Thumbprint, currentCertificate));
                    await ServerStore.Cluster.WaitForIndexNotification(res.Index);
                    first = false;
                }
                
                NoContentStatus();
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        [RavenAction("/admin/certificates", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            var thumbprint = GetQueryStringValueAndAssertIfSingleAndNotEmpty("thumbprint");

            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var clientCert = feature?.Certificate;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                if (clientCert != null && clientCert.Thumbprint.Equals(thumbprint))
                {
                    var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + thumbprint);
                    throw new InvalidOperationException($"Cannot delete {clientCertDef?.Name} becuase it's the current client certificate being used");
                }

                if (clientCert != null && Server.ClusterCertificateHolder.Certificate.Thumbprint.Equals(thumbprint))
                {
                    var serverCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + thumbprint);
                    throw new InvalidOperationException($"Cannot delete {serverCertDef?.Name} becuase it's the current server certificate being used");
                }

                var key = Constants.Certificates.Prefix + thumbprint;
                var definition = ReadCertificateFromCluster(ctx, key);
                if ((definition.SecurityClearance == SecurityClearance.ClusterAdmin || definition.SecurityClearance == SecurityClearance.ClusterNode) 
                    && IsClusterAdmin() == false)
                {
                    var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + clientCert?.Thumbprint);
                    throw new InvalidOperationException(
                        $"Cannot delete the certificate '{definition.Name}' with '{definition.SecurityClearance}' security clearance because the current client certificate being used has a lower clearance: {clientCertDef.SecurityClearance}");
                }

                if (string.IsNullOrEmpty(definition.CollectionPrimaryKey) == false)
                    throw new InvalidOperationException(
                        $"Cannot delete the certificate '{definition.Name}' with thumbprint '{definition.Thumbprint}'. You need to delete the primary certificate of the collection: {definition.CollectionPrimaryKey}");

                var keysToDelete = new List<string>
                {
                    key
                };
                keysToDelete.AddRange(definition.CollectionSecondaryKeys);

                await DeleteInternal(keysToDelete);
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
        }

        private CertificateDefinition ReadCertificateFromCluster(TransactionOperationContext ctx, string key)
        {
            using (ctx.OpenReadTransaction())
            {
                var certificate = ServerStore.Cluster.Read(ctx, key);
                if (certificate == null)
                    return null;

                return JsonDeserializationServer.CertificateDefinition(certificate);
            }
        }
        
        private async Task DeleteInternal(List<string> keys)
        {
            // Delete from cluster
            var res = await ServerStore.SendToLeaderAsync(new DeleteCertificateCollectionFromClusterCommand()
            {
                Names = keys
            });
            await ServerStore.Cluster.WaitForIndexNotification(res.Index);

            // Delete from local state
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                using (ctx.OpenWriteTransaction())
                {
                    ServerStore.Cluster.DeleteLocalState(ctx, keys);
                }
            }
        }

        [RavenAction("/admin/certificates", "GET", AuthorizationStatus.Operator)]
        public Task GetCertificates()
        {
            var thumbprint = GetStringQueryString("thumbprint", required: false);
            var showSecondary = GetBoolValueQueryString("secondary", required: false) ?? false;

            var start = GetStart();
            var pageSize = GetPageSize();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var certificates = new List<(string ItemName, BlittableJsonReaderObject Value)>();
                try
                {
                    if (string.IsNullOrEmpty(thumbprint))
                    {
                        foreach (var item in ServerStore.Cluster.ItemsStartingWith(context, Constants.Certificates.Prefix, start, pageSize))
                        {
                            var def = JsonDeserializationServer.CertificateDefinition(item.Value);

                            if (showSecondary || string.IsNullOrEmpty(def.CollectionPrimaryKey))
                                certificates.Add(item);
                        }
                    }
                    else
                    {
                        var key = Constants.Certificates.Prefix + thumbprint;
                        var certificate = ServerStore.Cluster.Read(context, key);
                        if (certificate == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return Task.CompletedTask;
                        }

                        var definition = JsonDeserializationServer.CertificateDefinition(certificate);
                        if (string.IsNullOrEmpty(definition.CollectionPrimaryKey) == false)
                        {
                            certificate = ServerStore.Cluster.Read(context, definition.CollectionPrimaryKey);
                            if (certificate == null)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                return Task.CompletedTask;
                            }
                        }

                        certificates.Add((key, certificate));
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray(context, "Results", certificates.ToArray(), (w, c, cert) =>
                        {
                            c.Write(w, cert.Value);
                        });
                        writer.WriteEndObject();
                    }
                }
                finally
                {
                    foreach (var cert in certificates)
                        cert.Value?.Dispose();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/certificates/whoami", "GET", AuthorizationStatus.ValidUser)]
        public Task WhoAmI()
        {
            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var clientCert = feature?.Certificate;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var certificate = ServerStore.Cluster.Read(ctx, Constants.Certificates.Prefix + clientCert?.Thumbprint);

                using (var writer = new BlittableJsonTextWriter(ctx, ResponseBodyStream()))
                {
                    writer.WriteObject(certificate);
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/certificates/edit", "POST", AuthorizationStatus.Operator)]
        public async Task Edit()
        {
            ServerStore.EnsureNotPassive();

            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var clientCert = feature?.Certificate;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var certificateJson = ctx.ReadForDisk(RequestBodyStream(), "edit-certificate"))
            {
                var newCertificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                ValidateCertificate(newCertificate, ServerStore);

                var key = Constants.Certificates.Prefix + newCertificate.Thumbprint;

                CertificateDefinition existingCertificate;
                using (ctx.OpenWriteTransaction())
                {
                    var certificate = ServerStore.Cluster.Read(ctx, key);
                    if (certificate == null)
                        throw new InvalidOperationException($"Cannot edit permissions for certificate with thumbprint '{newCertificate.Thumbprint}'. It doesn't exist in the cluster.");

                    existingCertificate = JsonDeserializationServer.CertificateDefinition(certificate);

                    if ((existingCertificate.SecurityClearance == SecurityClearance.ClusterAdmin || existingCertificate.SecurityClearance == SecurityClearance.ClusterNode) && IsClusterAdmin() == false)
                    {
                        var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + clientCert?.Thumbprint);
                        throw new InvalidOperationException($"Cannot edit the certificate '{existingCertificate.Name}'. It has '{existingCertificate.SecurityClearance}' security clearance while the current client certificate being used has a lower clearance: {clientCertDef.SecurityClearance}");
                    }

                    if ((newCertificate.SecurityClearance == SecurityClearance.ClusterAdmin || newCertificate.SecurityClearance == SecurityClearance.ClusterNode) && IsClusterAdmin() == false)
                    {
                        var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + clientCert?.Thumbprint);
                        throw new InvalidOperationException($"Cannot edit security clearance to '{newCertificate.SecurityClearance}' for certificate '{existingCertificate.Name}'. Only a 'Cluster Admin' can do that and your current client certificate has a lower clearance: {clientCertDef.SecurityClearance}");
                    }

                    ServerStore.Cluster.DeleteLocalState(ctx, key);
                }

                var deleteResult = await ServerStore.SendToLeaderAsync(new DeleteCertificateFromClusterCommand
                {
                    Name = Constants.Certificates.Prefix + existingCertificate.Thumbprint
                });
                await ServerStore.Cluster.WaitForIndexNotification(deleteResult.Index);

                var putResult = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + newCertificate.Thumbprint,
                    new CertificateDefinition
                    {
                        Name = newCertificate.Name,
                        Certificate = existingCertificate.Certificate,
                        Permissions = newCertificate.Permissions,
                        SecurityClearance = newCertificate.SecurityClearance,
                        Thumbprint = existingCertificate.Thumbprint
                    }));
                await ServerStore.Cluster.WaitForIndexNotification(putResult.Index);

                NoContentStatus();
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        [RavenAction("/admin/certificates/export", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task GetClusterCertificates()
        {
            var collection = new X509Certificate2Collection();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                List<(string ItemName, BlittableJsonReaderObject Value)> allItems = null;
                try
                {
                    allItems = ServerStore.Cluster.ItemsStartingWith(context, Constants.Certificates.Prefix, 0, int.MaxValue).ToList();
                    var clusterNodes = allItems.Select(item => JsonDeserializationServer.CertificateDefinition(item.Value))
                        .Where(certificateDef => certificateDef.SecurityClearance == SecurityClearance.ClusterNode)
                        .ToList();

                    if (clusterNodes.Count == 0)
                        throw new InvalidOperationException("Cannot get ClusterNode certificates, there should be at least one but it doesn't exist. This shouldn't happen!");

                    foreach (var cert in clusterNodes)
                    {
                        var x509Certificate2 = new X509Certificate2(Convert.FromBase64String(cert.Certificate));
                        collection.Import(x509Certificate2.Export(X509ContentType.Cert));
                    }
                }
                finally
                {
                    if (allItems != null)
                    {
                        foreach (var cert in allItems)
                            cert.Value?.Dispose();
                    }
                }
            }

            var pfx = collection.Export(X509ContentType.Pfx);

            var contentDisposition = "attachment; filename=ClusterCertificatesCollection.pfx";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            HttpContext.Response.ContentType = "binary/octet-stream";

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            HttpContext.Response.Body.Write(pfx, 0, pfx.Length);
            
            return Task.CompletedTask;
        }
        
        private static void ValidateCertificate(CertificateDefinition certificate, ServerStore serverStore)
        {
            if (string.IsNullOrWhiteSpace(certificate.Name))
                throw new ArgumentException($"{nameof(certificate.Name)} is a required field in the certificate definition");

            ValidatePermissions(certificate, serverStore);
        }

        private static void ValidatePermissions(CertificateDefinition certificate, ServerStore serverStore)
        {
            if (certificate.Permissions == null)
                throw new ArgumentException($"{nameof(certificate.Permissions)} is a required field in the certificate definition");

            foreach (var kvp in certificate.Permissions)
            {
                if (string.IsNullOrWhiteSpace(kvp.Key))
                    throw new ArgumentException("Error in permissions in the certificate definition, database name is empty");

                if (ResourceNameValidator.IsValidResourceName(kvp.Key, serverStore.Configuration.Core.DataDirectory.FullPath, out var errorMessage) == false)
                    throw new ArgumentException("Error in permissions in the certificate definition:" + errorMessage);

                if (kvp.Value != DatabaseAccess.ReadWrite && kvp.Value != DatabaseAccess.Admin)
                    throw new ArgumentException($"Error in permissions in the certificate definition, invalid access {kvp.Value} for database {kvp.Key}");
            }
        }
    }
}
