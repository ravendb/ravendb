using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Org.BouncyCastle.OpenSsl;
using Org.BouncyCastle.Pkcs;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Commercial;
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
using Sparrow.Utils;
using Voron.Platform.Posix;

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
            using (ctx.OpenReadTransaction())
            {
                var operationId = GetLongQueryString("operationId", false);
                if (operationId.HasValue == false)
                    operationId = ServerStore.Operations.GetNextOperationId();

                var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();

                var certificateJson = ctx.ReadForDisk(stream, "certificate-generation");

                var certificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                if (certificate.SecurityClearance == SecurityClearance.ClusterAdmin && IsClusterAdmin() == false)
                {
                    var clientCert = (HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection)?.Certificate;
                    var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + clientCert?.Thumbprint);
                    throw new InvalidOperationException($"Cannot generate the client certificate '{certificate.Name}' with 'Cluster Admin' security clearance because the current client certificate being used has a lower clearance: {clientCertDef.SecurityClearance}");
                }

                byte[] certs = null;
                await
                    ServerStore.Operations.AddOperation(
                        null,
                        "Generate certificate: " + certificate.Name,
                        Documents.Operations.Operations.OperationType.CertificateGeneration,
                        async onProgress =>
                        {
                            certs = await GenerateCertificateInternal(certificate, ServerStore);

                            return ClientCertificateGenerationResult.Instance;
                        },
                        operationId.Value);

                var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(certificate.Name) + ".zip";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "application/octet-stream";

                HttpContext.Response.Body.Write(certs, 0, certs.Length);
            }
        }

        public static async Task<byte[]> GenerateCertificateInternal(CertificateDefinition certificate, ServerStore serverStore)
        {
            ValidateCertificateDefinition(certificate, serverStore);

            if (serverStore.Server.Certificate?.Certificate == null)
            {
                var keys = new[]
                {
                    RavenConfiguration.GetKey(x => x.Security.CertificatePath),
                    RavenConfiguration.GetKey(x => x.Security.CertificateExec)
                };

                throw new InvalidOperationException($"Cannot generate the client certificate '{certificate.Name}' because the server certificate is not loaded. " +
                                                    $"You can supply a server certificate by using the following configuration keys: {string.Join(", ", keys)}" +
                                                    "For a more detailed explanation please read about authentication and certificates in the RavenDB documentation.");
            }

            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(certificate.Name, serverStore.Server.Certificate, out var clientCertBytes);

            var newCertDef = new CertificateDefinition
            {
                Name = certificate.Name,
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                Permissions = certificate.Permissions,
                SecurityClearance = certificate.SecurityClearance,
                Thumbprint = selfSignedCertificate.Thumbprint,
                NotAfter = selfSignedCertificate.NotAfter
            };

            var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + selfSignedCertificate.Thumbprint, newCertDef));
            await serverStore.Cluster.WaitForIndexNotification(res.Index);

            var ms = new MemoryStream();
            using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
            {
                var certBytes = selfSignedCertificate.Export(X509ContentType.Pfx, certificate.Password);

                var entry = archive.CreateEntry(certificate.Name + ".pfx");

                // Structure of the external attributes field: https://unix.stackexchange.com/questions/14705/the-zip-formats-external-file-attribute/14727#14727
                // The permissions go into the most significant 16 bits of an int
                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                using (var s = entry.Open())
                    s.Write(certBytes, 0, certBytes.Length);

                WriteCertificateAsPem(certificate.Name, clientCertBytes, certificate.Password, archive);
            }

            return ms.ToArray();
        }


        public static void WriteCertificateAsPem(string name, byte[] rawBytes, string exportPassword, ZipArchive archive)
        {
            var a = new Pkcs12Store();
            a.Load(new MemoryStream(rawBytes), Array.Empty<char>());

            X509CertificateEntry entry = null;
            AsymmetricKeyEntry key = null;
            foreach (var alias in a.Aliases)
            {
                var aliasKey = a.GetKey(alias.ToString());
                if (aliasKey != null)
                {
                    entry = a.GetCertificate(alias.ToString());
                    key = aliasKey;
                    break;
                }
            }

            if (entry == null)
            {
                throw new InvalidOperationException("Could not find private key.");
            }

            var zipEntryCrt = archive.CreateEntry(name + ".crt");
            zipEntryCrt.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

            using (var stream = zipEntryCrt.Open())
            using (var writer = new StreamWriter(stream))
            {
                var pw = new PemWriter(writer);
                pw.WriteObject(entry.Certificate);
            }

            var zipEntryKey = archive.CreateEntry(name + ".key");
            zipEntryKey.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

            using (var stream = zipEntryKey.Open())
            using (var writer = new StreamWriter(stream))
            {
                var pw = new PemWriter(writer);

                object privateKey;
                if (exportPassword != null)
                {
                    privateKey = new MiscPemGenerator(
                            key.Key,
                            "DES-EDE3-CBC",
                            exportPassword.ToCharArray(),
                            CertificateUtils.GetSeededSecureRandom())
                        .Generate();
                }
                else
                {
                    privateKey = key.Key;
                }

                pw.WriteObject(privateKey);

                writer.Flush();
            }
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

                ValidateCertificateDefinition(certificate, ServerStore);

                using (ctx.OpenReadTransaction())
                {
                    var clientCert = (HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection)?.Certificate;
                    var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + clientCert?.Thumbprint);

                    if ((certificate.SecurityClearance == SecurityClearance.ClusterAdmin || certificate.SecurityClearance == SecurityClearance.ClusterNode) && IsClusterAdmin() == false)
                        throw new InvalidOperationException($"Cannot save the certificate '{certificate.Name}' with '{certificate.SecurityClearance}' security clearance because the current client certificate being used has a lower clearance: {clientCertDef.SecurityClearance}");

                    if (string.IsNullOrWhiteSpace(certificate.Certificate))
                        throw new ArgumentException($"{nameof(certificate.Certificate)} is a mandatory property when saving an existing certificate");
                }

                byte[] certBytes;
                try
                {
                    certBytes = Convert.FromBase64String(certificate.Certificate);
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Unable to parse the {nameof(certificate.Certificate)} property, expected a Base64 value", e);
                }

                try
                {
                    var _ = string.IsNullOrEmpty(certificate.Password)
                        ? new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.MachineKeySet)
                        : new X509Certificate2(certBytes, certificate.Password, X509KeyStorageFlags.MachineKeySet);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Unable to load the provided certificate.", e);
                }

                try
                {
                    await PutCertificateCollectionInCluster(certificate, certBytes, certificate.Password, ServerStore, ctx);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to put certificate {certificate.Name} in the cluster.", e);
                }

                NoContentStatus();
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        public static async Task PutCertificateCollectionInCluster(CertificateDefinition certDef, byte[] certBytes, string password, ServerStore serverStore, TransactionOperationContext ctx)
        {
            var collection = new X509Certificate2Collection();

            if (string.IsNullOrEmpty(password))
                collection.Import(certBytes, (string)null, X509KeyStorageFlags.MachineKeySet);
            else
                collection.Import(certBytes, password, X509KeyStorageFlags.PersistKeySet | X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);

            var first = true;
            var collectionPrimaryKey = string.Empty;

            // we don't want to import items up the chain (signing keys)
            var issuers = new HashSet<string>();

            foreach (var x509Certificate in collection)
            {
                if (serverStore.Server.Certificate.Certificate?.Thumbprint != null && serverStore.Server.Certificate.Certificate.Thumbprint.Equals(x509Certificate.Thumbprint))
                    throw new InvalidOperationException($"You are trying to import the same server certificate ({x509Certificate.Thumbprint}) as the one which is already loaded. This is not supported.");

                if (x509Certificate.Issuer != x509Certificate.Subject)
                    issuers.Add(x509Certificate.Issuer);
            }

            foreach (var x509Certificate in collection)
            {
                if (issuers.Contains(x509Certificate.Subject))
                    continue;

                var currentCertDef = new CertificateDefinition
                {
                    Name = certDef.Name,
                    Permissions = certDef.Permissions,
                    SecurityClearance = certDef.SecurityClearance,
                    Password = certDef.Password,

                };

                if (x509Certificate.HasPrivateKey)
                {
                    // avoid storing the private key
                    currentCertDef.Certificate = Convert.ToBase64String(x509Certificate.Export(X509ContentType.Cert));
                }

                // In case of a collection, we group all the certificates together and treat them as one unit. 
                // They all have the same name and permissions but a different thumbprint.
                // The first certificate in the collection will be the primary certificate and its thumbprint will be the one shown in a GET request
                // The other certificates are secondary certificates and will contain a link to the primary certificate.
                currentCertDef.Thumbprint = x509Certificate.Thumbprint;
                currentCertDef.NotAfter = x509Certificate.NotAfter;
                currentCertDef.Certificate = Convert.ToBase64String(x509Certificate.Export(X509ContentType.Cert));

                if (first)
                {
                    var firstKey = Constants.Certificates.Prefix + x509Certificate.Thumbprint;
                    collectionPrimaryKey = firstKey;

                    foreach (var cert in collection)
                    {
                        if (issuers.Contains(cert.Subject))
                            continue;

                        if (Constants.Certificates.Prefix + cert.Thumbprint != firstKey)
                            currentCertDef.CollectionSecondaryKeys.Add(Constants.Certificates.Prefix + cert.Thumbprint);
                    }
                }
                else
                    currentCertDef.CollectionPrimaryKey = collectionPrimaryKey;

                var certKey = Constants.Certificates.Prefix + currentCertDef.Thumbprint;
                if (serverStore.CurrentRachisState == RachisState.Passive)
                {
                    using (var certificate = ctx.ReadObject(currentCertDef.ToJson(), "Client/Certificate/Definition"))
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        serverStore.Cluster.PutLocalState(ctx, certKey, certificate);
                        tx.Commit();
                    }
                }
                else
                {
                    var putResult = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(certKey, currentCertDef));
                    await serverStore.Cluster.WaitForIndexNotification(putResult.Index);
                }

                first = false;
            }
        }

        [RavenAction("/admin/certificates", "DELETE", AuthorizationStatus.Operator)]
        public async Task Delete()
        {
            var thumbprint = GetQueryStringValueAndAssertIfSingleAndNotEmpty("thumbprint");

            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var clientCert = feature?.Certificate;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                if (clientCert?.Thumbprint != null && clientCert.Thumbprint.Equals(thumbprint))
                {
                    var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + thumbprint);
                    throw new InvalidOperationException($"Cannot delete {clientCertDef?.Name} because it's the current client certificate being used");
                }

                if (clientCert != null && Server.Certificate.Certificate?.Thumbprint != null && Server.Certificate.Certificate.Thumbprint.Equals(thumbprint))
                {
                    var serverCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + thumbprint);
                    throw new InvalidOperationException($"Cannot delete {serverCertDef?.Name} because it's the current server certificate being used");
                }

                var key = Constants.Certificates.Prefix + thumbprint;
                var definition = ReadCertificateFromCluster(ctx, key);
                if (definition != null && (definition.SecurityClearance == SecurityClearance.ClusterAdmin || definition.SecurityClearance == SecurityClearance.ClusterNode)
                    && IsClusterAdmin() == false)
                {
                    var clientCertDef = ReadCertificateFromCluster(ctx, Constants.Certificates.Prefix + clientCert?.Thumbprint);
                    throw new InvalidOperationException(
                        $"Cannot delete the certificate '{definition.Name}' with '{definition.SecurityClearance}' security clearance because the current client certificate being used has a lower clearance: {clientCertDef.SecurityClearance}");
                }

                if (string.IsNullOrEmpty(definition?.CollectionPrimaryKey) == false)
                    throw new InvalidOperationException(
                        $"Cannot delete the certificate '{definition.Name}' with thumbprint '{definition.Thumbprint}'. You need to delete the primary certificate of the collection: {definition.CollectionPrimaryKey}");

                var keysToDelete = new List<string>
                {
                    key
                };

                if (definition != null)
                    keysToDelete.AddRange(definition.CollectionSecondaryKeys);

                await DeleteInternal(keysToDelete);
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
        }

        private CertificateDefinition ReadCertificateFromCluster(TransactionOperationContext ctx, string key)
        {
            var certificate = ServerStore.Cluster.Read(ctx, key);
            if (certificate == null)
                return null;

            return JsonDeserializationServer.CertificateDefinition(certificate);
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
                var certificateList = new Dictionary<string, BlittableJsonReaderObject>();

                try
                {
                    if (string.IsNullOrEmpty(thumbprint))
                    {
                        // The server cert is not part of the local state or the cluster certificates, we add it to the list separately
                        var serverCertKey = Constants.Certificates.Prefix + Server.Certificate.Certificate?.Thumbprint;
                        if (Server.Certificate.Certificate != null)
                        {
                            var serverCertDef = new CertificateDefinition
                            {
                                Name = "Server Certificate",
                                Certificate = Convert.ToBase64String(Server.Certificate.Certificate.Export(X509ContentType.Cert)),
                                Permissions = new Dictionary<string, DatabaseAccess>(),
                                SecurityClearance = SecurityClearance.ClusterNode,
                                Thumbprint = Server.Certificate.Certificate.Thumbprint,
                                NotAfter = Server.Certificate.Certificate.NotAfter
                            };

                            var serverCert = context.ReadObject(serverCertDef.ToJson(), "Server/Certificate/Definition");

                            certificateList.TryAdd(serverCertKey, serverCert);
                        }

                        // If we are passive, we take the certs from the local state
                        if (ServerStore.CurrentRachisState == RachisState.Passive)
                        {
                            List<string> localCertKeys;

                            localCertKeys = ServerStore.Cluster.GetCertificateKeysFromLocalState(context).ToList();
                            
                            foreach (var localCertKey in localCertKeys)
                            {
                                var localCertificate = ServerStore.Cluster.GetLocalState(context, localCertKey);
                                if (localCertificate == null)
                                    continue;

                                var def = JsonDeserializationServer.CertificateDefinition(localCertificate);

                                if (showSecondary || string.IsNullOrEmpty(def.CollectionPrimaryKey))
                                    certificateList.TryAdd(localCertKey, localCertificate);
                                else
                                    localCertificate.Dispose();
                            }
                            
                        }
                        // If we are not passive, we take the certs from the cluster
                        else
                        {
                            foreach (var item in ServerStore.Cluster.ItemsStartingWith(context, Constants.Certificates.Prefix, start, pageSize))
                            {
                                var def = JsonDeserializationServer.CertificateDefinition(item.Value);

                                if (showSecondary || string.IsNullOrEmpty(def.CollectionPrimaryKey))
                                    certificateList.TryAdd(item.ItemName, item.Value);
                                else
                                    item.Value.Dispose();
                            }
                        }
                    }
                    else
                    {
                        var key = Constants.Certificates.Prefix + thumbprint;

                        var certificate = ServerStore.CurrentRachisState == RachisState.Passive
                            ? ServerStore.Cluster.GetLocalState(context, key)
                            : ServerStore.Cluster.Read(context, key);

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

                        certificateList.TryAdd(key, certificate);
                    }
                    

                    var wellKnown = ServerStore.Configuration.Security.WellKnownAdminCertificates;

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray(context, "Results", certificateList.ToArray(), (w, c, cert) =>
                        {
                            c.Write(w, cert.Value);
                        });
                        writer.WriteComma();
                        writer.WritePropertyName("LoadedServerCert");
                        writer.WriteString(Server.Certificate.Certificate?.Thumbprint);
                        writer.WriteComma();
                        writer.WriteArray("WellKnownAdminCerts", wellKnown);
                        writer.WriteEndObject();
                    }
                }
                finally
                {
                    foreach (var cert in certificateList)
                        cert.Value?.Dispose();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/certificates/whoami", "GET", AuthorizationStatus.ValidUser)]
        public Task WhoAmI()
        {
            var clientCert = GetCurrentCertificate();

            if (clientCert == null)
            {
                return NoContent();
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var certKey = Constants.Certificates.Prefix + clientCert.Thumbprint;
                var certificate = ServerStore.Cluster.Read(ctx, certKey) ??
                                  ServerStore.Cluster.GetLocalState(ctx, certKey);

                if (certificate == null)
                {
                    // The client certificate is not registered in the ServerStore.
                    // Let's check if the client is using the server certificate or one of the well known admin certs.

                    var wellKnown = ServerStore.Configuration.Security.WellKnownAdminCertificates;

                    if (clientCert.Equals(Server.Certificate.Certificate))
                    {
                        if (Server.Certificate.Certificate != null)
                        {
                            var serverCertDef = new CertificateDefinition
                            {
                                Name = "Server Certificate",
                                Certificate = Convert.ToBase64String(Server.Certificate.Certificate.Export(X509ContentType.Cert)),
                                Permissions = new Dictionary<string, DatabaseAccess>(),
                                SecurityClearance = SecurityClearance.ClusterNode,
                                Thumbprint = Server.Certificate.Certificate.Thumbprint,
                                NotAfter = Server.Certificate.Certificate.NotAfter
                            };

                            certificate = ctx.ReadObject(serverCertDef.ToJson(), "Server/Certificate/Definition");
                        }
                    }
                    else if (wellKnown != null && wellKnown.Contains(clientCert.Thumbprint, StringComparer.OrdinalIgnoreCase))
                    {
                        var serverCertDef = new CertificateDefinition
                        {
                            Name = "Well Known Admin Certificate",
                            Permissions = new Dictionary<string, DatabaseAccess>(),
                            SecurityClearance = SecurityClearance.ClusterAdmin,
                            Thumbprint = clientCert.Thumbprint,
                        };
                        certificate = ctx.ReadObject(serverCertDef.ToJson(), "WellKnown/Certificate/Definition");
                    }
                }

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

                ValidateCertificateDefinition(newCertificate, ServerStore);

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

                var putResult = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + newCertificate.Thumbprint,
                    new CertificateDefinition
                    {
                        Name = newCertificate.Name,
                        Certificate = existingCertificate.Certificate,
                        Permissions = newCertificate.Permissions,
                        SecurityClearance = newCertificate.SecurityClearance,
                        Thumbprint = existingCertificate.Thumbprint,
                        NotAfter = existingCertificate.NotAfter
                    }));
                await ServerStore.Cluster.WaitForIndexNotification(putResult.Index);

                NoContentStatus();
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        [RavenAction("/admin/certificates/export", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task GetClusterCertificates()
        {
            if (Server.Certificate.Certificate == null)
                return Task.CompletedTask;

            var collection = new X509Certificate2Collection();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                collection.Import(Server.Certificate.Certificate.Export(X509ContentType.Cert), (string)null, X509KeyStorageFlags.MachineKeySet);

                if (ServerStore.CurrentRachisState != RachisState.Passive)
                {
                    using (context.OpenReadTransaction())
                    {
                        List<(string ItemName, BlittableJsonReaderObject Value)> allItems = null;
                        try
                        {
                            allItems = ServerStore.Cluster.ItemsStartingWith(context, Constants.Certificates.Prefix, 0, int.MaxValue).ToList();
                            var clusterNodes = allItems.Select(item => JsonDeserializationServer.CertificateDefinition(item.Value))
                                .Where(certificateDef => certificateDef.SecurityClearance == SecurityClearance.ClusterNode)
                                .ToList();
                            
                            foreach (var cert in clusterNodes)
                            {
                                var x509Certificate2 = new X509Certificate2(Convert.FromBase64String(cert.Certificate), (string)null, X509KeyStorageFlags.MachineKeySet);

                                if (collection.Contains(x509Certificate2) == false)
                                    collection.Import(x509Certificate2.Export(X509ContentType.Cert), (string)null, X509KeyStorageFlags.MachineKeySet);
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
                }
            }

            var pfx = collection.Export(X509ContentType.Pfx);

            var contentDisposition = "attachment; filename=ClusterCertificatesCollection.pfx";
            HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
            HttpContext.Response.ContentType = "application/octet-stream";

            HttpContext.Response.Body.Write(pfx, 0, pfx.Length);

            return Task.CompletedTask;
        }

        [RavenAction("/admin/certificates/mode", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task Mode()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                using (var writer = new BlittableJsonTextWriter(ctx, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("SetupMode");
                    writer.WriteString(ServerStore.Configuration.Core.SetupMode.ToString());
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/certificates/cluster-domains", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task ClusterDomains()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                List<string> domains = null;
                if (ServerStore.CurrentRachisState != RachisState.Passive)
                {
                    ClusterTopology clusterTopology;
                    clusterTopology = ServerStore.GetClusterTopology(context);
                    domains = clusterTopology.AllNodes.Select(node => new Uri(node.Value).DnsSafeHost).ToList();
                }
                else
                {
                    var myUrl = Server.Configuration.Core.PublicServerUrl.HasValue
                        ? Server.Configuration.Core.PublicServerUrl.Value.UriValue
                        : Server.Configuration.Core.ServerUrls[0];
                    var myDomain = new Uri(myUrl).DnsSafeHost;
                    domains = new List<string>
                    {
                        myDomain
                    };
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("ClusterDomains");

                    writer.WriteStartArray();
                    var first = true;
                    foreach (var domain in domains)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;
                        writer.WriteString(domain);
                    }
                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/certificates/replacement/reset", "POST", AuthorizationStatus.ClusterAdmin)]
        public Task ReplacementReset()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                ServerStore.Cluster.DeleteItem(context, CertificateReplacement.CertificateReplacementDoc);
                tx.Commit();
            }

            return NoContent();
        }

        [RavenAction("/admin/certificates/replacement/status", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task ReplacementStatus()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                using (context.OpenReadTransaction())
                {
                    var certStatus = ServerStore.Cluster.GetItem(context, CertificateReplacement.CertificateReplacementDoc);

                    if (certStatus != null)
                    {
                        certStatus.TryGet(nameof(CertificateReplacement.Confirmations), out int confirmations);
                        certStatus.TryGet(nameof(CertificateReplacement.Thumbprint), out string thumbprint);
                        certStatus.TryGet(nameof(CertificateReplacement.OldThumbprint), out string oldThumbprint);
                        certStatus.TryGet(nameof(CertificateReplacement.ReplaceImmediately), out bool replaceImmediately);
                        certStatus.TryGet(nameof(CertificateReplacement.Replaced), out int replaced);

                        // Not writing the certificate itself, because it has the private key
                        writer.WriteStartObject();
                        writer.WritePropertyName(nameof(CertificateReplacement.Confirmations));
                        writer.WriteInteger(confirmations);
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(CertificateReplacement.Thumbprint));
                        writer.WriteString(thumbprint);
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(CertificateReplacement.OldThumbprint));
                        writer.WriteString(oldThumbprint);
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(CertificateReplacement.ReplaceImmediately));
                        writer.WriteBool(replaceImmediately);
                        writer.WriteComma();
                        writer.WritePropertyName(nameof(CertificateReplacement.Replaced));
                        writer.WriteInteger(replaced);
                        writer.WriteEndObject();
                    }
                    else
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    }
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/certificates/letsencrypt/renewal-date", "GET", AuthorizationStatus.ClusterAdmin)]
        public Task RenewalDate()
        {
            if (ServerStore.Configuration.Core.SetupMode != SetupMode.LetsEncrypt)
                throw new InvalidOperationException("This server wasn't set up using the Let's Encrypt setup mode.");

            if (Server.Certificate.Certificate == null)
                throw new InvalidOperationException("The server certificate is not loaded.");

            var (_, renewalDate) = Server.CalculateRenewalDate(Server.Certificate, false);
            
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("EstimatedRenewal");
                writer.WriteDateTime(renewalDate, true);
                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/certificates/letsencrypt/force-renew", "OPTIONS", AuthorizationStatus.ClusterAdmin)]
        [RavenAction("/admin/certificates/refresh", "OPTIONS", AuthorizationStatus.ClusterAdmin)]
        [RavenAction("/admin/certificates/replace-cluster-cert", "OPTIONS", AuthorizationStatus.ClusterAdmin)]
        public Task AllowPreflightRequest()
        {
            SetupCORSHeaders();
            HttpContext.Response.Headers.Remove("Content-Type");
            return Task.CompletedTask;
        }

        [RavenAction("/admin/certificates/letsencrypt/force-renew", "POST", AuthorizationStatus.ClusterAdmin)]
        public Task ForceRenew()
        {
            SetupCORSHeaders();

            if (ServerStore.IsLeader())
            {
                if (ServerStore.Configuration.Core.SetupMode != SetupMode.LetsEncrypt)
                    throw new InvalidOperationException("Cannot force renew for this server certificate. This server wasn't set up using the Let's Encrypt setup mode.");

                if (Server.Certificate.Certificate == null)
                    throw new InvalidOperationException("Cannot force renew this Let's Encrypt server certificate. The server certificate is not loaded.");

                try
                {
                    var success = Server.RefreshClusterCertificate(true);
                    using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName(nameof(ForceRenewResult.Success));
                        writer.WriteBool(success);
                        writer.WriteEndObject();
                    }

                    return Task.CompletedTask;
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to force renew the Let's Encrypt server certificate for domain: {Server.Certificate.Certificate.GetNameInfo(X509NameType.SimpleName, false)}", e);
                }
            }
            RedirectToLeader();
            return Task.CompletedTask;
        }

        [RavenAction("/admin/certificates/refresh", "POST", AuthorizationStatus.ClusterAdmin)]
        public Task TriggerCertificateRefresh()
        {
            SetupCORSHeaders();

            if (ServerStore.IsLeader() || ServerStore.Configuration.Core.SetupMode != SetupMode.LetsEncrypt)
            {
                // What we do here is trigger the refresh cycle which normally happens once an hour.

                // The difference between this and /admin/certificates/letsencrypt/force-renew is that here we also allow it for non-LetsEncrypt setups
                // in which case we'll check if the certificate changed on disk and if so we'll update it immediately on the local node (only)

                var replaceImmediately = GetBoolValueQueryString("replaceImmediately", required: false) ?? false;

                if (Server.Certificate.Certificate == null)
                    throw new InvalidOperationException("Failed to trigger a certificate refresh cycle. The server certificate is not loaded.");

                try
                {
                    Server.RefreshClusterCertificate(replaceImmediately);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Failed to trigger a certificate refresh cycle", e);
                }

                return NoContent();
            }
            RedirectToLeader();
            return Task.CompletedTask;
        }

        [RavenAction("/admin/certificates/replace-cluster-cert", "POST", AuthorizationStatus.ClusterAdmin)]
        public async Task ReplaceClusterCert()
        {
            SetupCORSHeaders();

            if (ServerStore.IsLeader())
            {
                var replaceImmediately = GetBoolValueQueryString("replaceImmediately", required: false) ?? false;

                ServerStore.EnsureNotPassive();
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (var certificateJson = ctx.ReadForDisk(RequestBodyStream(), "replace-cluster-cert"))
                {
                    try
                    {
                        var certificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                        if (string.IsNullOrWhiteSpace(certificate.Certificate))
                            throw new ArgumentException($"{nameof(certificate.Certificate)} is a required field in the certificate definition.");

                        // Load the password protected certificate and export it without a password, to send it through the cluster.
                        if (string.IsNullOrWhiteSpace(certificate.Password) == false)
                        {
                            try
                            {
                                var cert = new X509Certificate2Collection();
                                var certBytes = Convert.FromBase64String(certificate.Certificate);
                                cert.Import(certBytes, certificate.Password, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
                                // Exporting with the private key, but without the password
                                certificate.Certificate = Convert.ToBase64String(cert.Export(X509ContentType.Pkcs12));
                            }
                            catch (Exception e)
                            {
                                throw new ArgumentException("Failed to load the password protected certificate and export it back. Is the password correct?", e);
                            }
                        }

                        // Ensure we'll be able to load the certificate
                        try
                        {
                            var certBytes = Convert.FromBase64String(certificate.Certificate);
                            var _ = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
                        }
                        catch (Exception e)
                        {
                            throw new ArgumentException("Unable to load the provided certificate.", e);
                        }

                        if (IsClusterAdmin() == false)
                            throw new InvalidOperationException("Cannot replace the server certificate. Only a ClusterAdmin can do this.");

                        var timeoutTask = TimeoutManager.WaitFor(TimeSpan.FromSeconds(60), ServerStore.ServerShutdown);

                        var replicationTask = Server.StartCertificateReplicationAsync(certificate.Certificate, replaceImmediately);

                        await Task.WhenAny(replicationTask, timeoutTask);
                        if (replicationTask.IsCompleted == false)
                            throw new TimeoutException("Timeout when trying to replace the server certificate.");
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to replace the server certificate.", e);
                    }
                }

                NoContentStatus();
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                return;
            }
            
            RedirectToLeader();
        }

        public static void ValidateCertificateDefinition(CertificateDefinition certificate, ServerStore serverStore)
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
