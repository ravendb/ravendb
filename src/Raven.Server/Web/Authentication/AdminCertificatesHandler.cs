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
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions.Security;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Web.Authentication
{
    public class AdminCertificatesHandler : ServerRequestHandler
    {
        [RavenAction("/admin/certificates", "POST", AuthorizationStatus.Operator, DisableOnCpuCreditsExhaustion = true)]
        public async Task Generate()
        {
            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node
            await ServerStore.EnsureNotPassiveAsync();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var operationId = GetLongQueryString("operationId", false);
                if (operationId.HasValue == false)
                    operationId = ServerStore.Operations.GetNextOperationId();

                var stream = TryGetRequestFromStream("Options") ?? RequestBodyStream();

                var certificateJson = await ctx.ReadForDiskAsync(stream, "certificate-generation");

                var certificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                if (certificate.SecurityClearance == SecurityClearance.ClusterAdmin && IsClusterAdmin() == false)
                {
                    var clientCert = (HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection)?.Certificate;
                    var clientCertDef = ReadCertificateFromCluster(ctx, clientCert?.Thumbprint);
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
                            certs = await GenerateCertificateInternal(certificate, ServerStore, GetRaftRequestIdFromQuery());

                            return ClientCertificateGenerationResult.Instance;
                        },
                        operationId.Value);

                var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(certificate.Name) + ".zip";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "application/octet-stream";

                await HttpContext.Response.Body.WriteAsync(certs, 0, certs.Length);
            }
        }

        public static async Task<byte[]> GenerateCertificateInternal(CertificateDefinition certificate, ServerStore serverStore, string raftRequestId)
        {
            ValidateCertificateDefinition(certificate, serverStore);

            if (serverStore.Server.Certificate?.Certificate == null)
            {
                var keys = new[]
                {
                    RavenConfiguration.GetKey(x => x.Security.CertificatePath),
                    RavenConfiguration.GetKey(x => x.Security.CertificateLoadExec)
                };

                throw new InvalidOperationException($"Cannot generate the client certificate '{certificate.Name}' because the server certificate is not loaded. " +
                                                    $"You can supply a server certificate by using the following configuration keys: {string.Join(", ", keys)}" +
                                                    "For a more detailed explanation please read about authentication and certificates in the RavenDB documentation.");
            }

            // this creates a client certificate which is signed by the current server certificate
            var selfSignedCertificate = CertificateUtils.CreateSelfSignedClientCertificate(certificate.Name, serverStore.Server.Certificate, out var clientCertBytes,
                certificate.NotAfter ?? DateTime.UtcNow.Date.AddYears(5));

            var newCertDef = new CertificateDefinition
            {
                Name = certificate.Name,
                // this does not include the private key, that is only for the client
                Certificate = Convert.ToBase64String(selfSignedCertificate.Export(X509ContentType.Cert)),
                Permissions = certificate.Permissions,
                SecurityClearance = certificate.SecurityClearance,
                Thumbprint = selfSignedCertificate.Thumbprint,
                PublicKeyPinningHash = selfSignedCertificate.GetPublicKeyPinningHash(),
                NotAfter = selfSignedCertificate.NotAfter
            };

            var res = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(selfSignedCertificate.Thumbprint, newCertDef, raftRequestId));
            await serverStore.Cluster.WaitForIndexNotification(res.Index);

            var ms = new MemoryStream();
            using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
            {
                var certBytes = selfSignedCertificate.Export(X509ContentType.Pfx, certificate.Password);

                var entry = archive.CreateEntry(certificate.Name + ".pfx");

                // Structure of the external attributes field: https://unix.stackexchange.com/questions/14705/the-zip-formats-external-file-attribute/14727#14727
                // The permissions go into the most significant 16 bits of an int
                entry.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

                await using (var s = entry.Open())
                    await s.WriteAsync(certBytes, 0, certBytes.Length);

                await WriteCertificateAsPemAsync(certificate.Name, clientCertBytes, certificate.Password, archive);
            }

            return ms.ToArray();
        }

        public static async Task WriteCertificateAsPemAsync(string name, byte[] rawBytes, string exportPassword, ZipArchive archive)
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

            await using (var stream = zipEntryCrt.Open())
            await using (var writer = new StreamWriter(stream))
            {
                var pw = new PemWriter(writer);
                pw.WriteObject(entry.Certificate);
            }

            var zipEntryKey = archive.CreateEntry(name + ".key");
            zipEntryKey.ExternalAttributes = ((int)(FilePermissions.S_IRUSR | FilePermissions.S_IWUSR)) << 16;

            await using (var stream = zipEntryKey.Open())
            await using (var writer = new StreamWriter(stream))
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

                await writer.FlushAsync();
            }
        }

        [RavenAction("/admin/certificates", "PUT", AuthorizationStatus.Operator)]
        public async Task Put()
        {
            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node

            await ServerStore.EnsureNotPassiveAsync();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var certificateJson = await ctx.ReadForDiskAsync(RequestBodyStream(), "put-certificate"))
            {
                var certificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                ValidateCertificateDefinition(certificate, ServerStore);

                using (ctx.OpenReadTransaction())
                {
                    var clientCert = (HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection)?.Certificate;
                    var clientCertDef = ReadCertificateFromCluster(ctx, clientCert?.Thumbprint);

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
                    var password = string.IsNullOrEmpty(certificate.Password) ? null : certificate.Password;
                    using var certificate2 = new X509Certificate2(certBytes, password, X509KeyStorageFlags.MachineKeySet);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Unable to load the provided certificate.", e);
                }

                try
                {
                    await PutCertificateCollectionInCluster(certificate, certBytes, certificate.Password, ServerStore, ctx, GetRaftRequestIdFromQuery());
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to put certificate {certificate.Name} in the cluster.", e);
                }

                NoContentStatus(HttpStatusCode.Created);
            }
        }

        public static async Task PutCertificateCollectionInCluster(CertificateDefinition certDef, byte[] certBytes, string password, ServerStore serverStore, TransactionOperationContext ctx, string raftRequestId)
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
                    Password = certDef.Password
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
                currentCertDef.PublicKeyPinningHash = x509Certificate.GetPublicKeyPinningHash();
                currentCertDef.NotAfter = x509Certificate.NotAfter;
                currentCertDef.Certificate = Convert.ToBase64String(x509Certificate.Export(X509ContentType.Cert));

                if (first)
                {
                    var firstKey = x509Certificate.Thumbprint;
                    collectionPrimaryKey = firstKey;

                    foreach (var cert in collection)
                    {
                        if (issuers.Contains(cert.Subject))
                            continue;

                        if (cert.Thumbprint != firstKey)
                            currentCertDef.CollectionSecondaryKeys.Add(cert.Thumbprint);
                    }
                }
                else
                    currentCertDef.CollectionPrimaryKey = collectionPrimaryKey;

                var certKey = currentCertDef.Thumbprint;
                if (serverStore.CurrentRachisState == RachisState.Passive)
                {
                    using (var certificate = ctx.ReadObject(currentCertDef.ToJson(), "Client/Certificate/Definition"))
                    using (var tx = ctx.OpenWriteTransaction())
                    {
                        serverStore.Cluster.PutLocalState(ctx, certKey, certificate, currentCertDef);
                        tx.Commit();
                    }
                }
                else
                {
                    var putResult = await serverStore.PutValueInClusterAsync(new PutCertificateCommand(certKey, currentCertDef, $"{raftRequestId}/{certKey}"));
                    await serverStore.Cluster.WaitForIndexNotification(putResult.Index);
                }

                first = false;
            }
        }

        [RavenAction("/admin/certificates/purge", "DELETE", AuthorizationStatus.Operator)]
        public async Task PurgeExpired()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var allCerts = new Dictionary<string, BlittableJsonReaderObject>();

                GetAllRegisteredCertificates(context, allCerts, false);

                var keysToDelete = new List<string>();

                foreach (var cert in allCerts)
                {
                    if (cert.Value.TryGet(nameof(CertificateDefinition.NotAfter), out DateTime notAfter) && DateTime.UtcNow > notAfter)
                        keysToDelete.Add(cert.Key);
                }

                await DeleteInternal(keysToDelete, GetRaftRequestIdFromQuery());
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
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
                    var clientCertDef = ReadCertificateFromCluster(ctx, thumbprint);
                    throw new InvalidOperationException($"Cannot delete {clientCertDef?.Name} because it's the current client certificate being used");
                }

                var definition = ReadCertificateFromCluster(ctx, thumbprint);
                if (definition != null && (definition.SecurityClearance == SecurityClearance.ClusterAdmin || definition.SecurityClearance == SecurityClearance.ClusterNode)
                    && IsClusterAdmin() == false)
                {
                    var clientCertDef = ReadCertificateFromCluster(ctx, clientCert?.Thumbprint);
                    throw new InvalidOperationException(
                        $"Cannot delete the certificate '{definition.Name}' with '{definition.SecurityClearance}' security clearance because the current client certificate being used has a lower clearance: {clientCertDef.SecurityClearance}");
                }

                if (string.IsNullOrEmpty(definition?.CollectionPrimaryKey) == false)
                    throw new InvalidOperationException(
                        $"Cannot delete the certificate '{definition.Name}' with thumbprint '{definition.Thumbprint}'. You need to delete the primary certificate of the collection: {definition.CollectionPrimaryKey}");

                var keysToDelete = new List<string>
                {
                    thumbprint
                };

                if (definition != null)
                    keysToDelete.AddRange(definition.CollectionSecondaryKeys);

                await DeleteInternal(keysToDelete, GetRaftRequestIdFromQuery());
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
        }

        private CertificateDefinition ReadCertificateFromCluster(TransactionOperationContext ctx, string key)
        {
            var certificate = ServerStore.Cluster.GetCertificateByThumbprint(ctx, key);
            if (certificate == null)
                return null;

            return JsonDeserializationServer.CertificateDefinition(certificate);
        }

        private async Task DeleteInternal(List<string> keys, string requestId)
        {
            // Delete from cluster
            var res = await ServerStore.SendToLeaderAsync(new DeleteCertificateCollectionFromClusterCommand(requestId)
            {
                Names = keys
            });
            await ServerStore.Cluster.WaitForIndexNotification(res.Index);

            // Delete from local state
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                using (var tx = ctx.OpenWriteTransaction())
                {
                    ServerStore.Cluster.DeleteLocalState(ctx, keys);
                    tx.Commit();
                }
            }
        }

        [RavenAction("/admin/certificates", "GET", AuthorizationStatus.Operator)]
        public async Task GetCertificates()
        {
            var thumbprint = GetStringQueryString("thumbprint", required: false);
            var name = GetStringQueryString("name", required: false);
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            var includeSecondary = GetBoolValueQueryString("secondary", required: false) ?? false;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var certificateList = new Dictionary<string, BlittableJsonReaderObject>(StringComparer.OrdinalIgnoreCase);

                try
                {
                    if (string.IsNullOrEmpty(thumbprint))
                    {
                        const string serverCertificateName = "Server Certificate";
                        // The server cert is not part of the local state or the cluster certificates, we add it to the list separately
                        if ((name == null || name == serverCertificateName) && Server.Certificate.Certificate != null)
                        {
                            var serverCertDef = new CertificateDefinition
                            {
                                Name = serverCertificateName,
                                Certificate = Convert.ToBase64String(Server.Certificate.Certificate.Export(X509ContentType.Cert)),
                                Permissions = new Dictionary<string, DatabaseAccess>(),
                                SecurityClearance = SecurityClearance.ClusterNode,
                                Thumbprint = Server.Certificate.Certificate.Thumbprint,
                                PublicKeyPinningHash = Server.Certificate.Certificate.GetPublicKeyPinningHash(),
                                NotAfter = Server.Certificate.Certificate.NotAfter
                            };

                            var serverCert = context.ReadObject(serverCertDef.ToJson(metadataOnly), "Server/Certificate/Definition");

                            certificateList.TryAdd(Server.Certificate.Certificate?.Thumbprint, serverCert);
                        }

                        GetAllRegisteredCertificates(context, certificateList, includeSecondary, name, metadataOnly);
                    }
                    else
                    {
                        if (TryGetAndAddCertificateByThumbprint(context, certificateList, thumbprint, metadataOnly) == false)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return;
                        }
                    }

                    var wellKnown = ServerStore.Configuration.Security.WellKnownAdminCertificates;

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray(context, "Results", certificateList.ToArray(), (w, c, cert) => c.Write(w, cert.Value));
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
        }

        private bool TryGetCertificateByThumbprint(TransactionOperationContext context, string thumbprint, out BlittableJsonReaderObject certificate)
        {
            certificate = ServerStore.CurrentRachisState == RachisState.Passive
                ? ServerStore.Cluster.GetLocalStateByThumbprint(context, thumbprint)
                : ServerStore.Cluster.GetCertificateByThumbprint(context, thumbprint);
            return certificate != null;
        }

        private bool TryGetAndAddCertificateByThumbprint(TransactionOperationContext context, Dictionary<string, BlittableJsonReaderObject> certificateList,
            string thumbprint, bool metadataOnly)
        {
            if (TryGetCertificateByThumbprint(context, thumbprint, out var certificate) == false)
                return false;

            var definition = JsonDeserializationServer.CertificateDefinition(certificate);
            if (string.IsNullOrEmpty(definition.CollectionPrimaryKey) == false)
            {
                certificate.Dispose();
                if (TryGetCertificateByThumbprint(context, definition.CollectionPrimaryKey, out certificate) == false)
                    return false;
            }

            if (metadataOnly)
            {
                certificate.Dispose();
                certificate = context.ReadObject(definition.ToJson(true), "Client/Certificate/Definition");
            }

            certificateList.TryAdd(thumbprint, certificate);
            return true;
        }

        private void GetAllRegisteredCertificates(
            TransactionOperationContext context,
            Dictionary<string, BlittableJsonReaderObject> certificates,
            bool includeSecondary,
            string name = null,
            bool metadataOnly = false)
        {
            var localCertificates = ServerStore.CurrentRachisState == RachisState.Passive
                // If we are passive, we take the certs from the local state
                ? ClusterStateMachine.GetAllCertificatesFromLocalState(context)
                : ClusterStateMachine.GetAllCertificatesFromCluster(context, GetStart(), GetPageSize());

            foreach (var (thumbprint, certificate) in localCertificates)
            {
                var def = JsonDeserializationServer.CertificateDefinition(certificate);
                if (name != null && name != def.Name || includeSecondary == false && string.IsNullOrEmpty(def.CollectionPrimaryKey) == false)
                {
                    certificate.Dispose();
                    continue;
                }

                var certificateRef = certificate;
                if (metadataOnly)
                {
                    certificate.Dispose();
                    certificateRef = context.ReadObject(def.ToJson(true), "Client/Certificate/Definition");
                }
                certificates.TryAdd(thumbprint, certificateRef);
            }
        }

        [RavenAction("/certificates/whoami", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task WhoAmI()
        {
            var clientCert = GetCurrentCertificate();

            if (clientCert == null)
            {
                NoContentStatus();
                return;
            }

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var certificate = ServerStore.Cluster.GetCertificateByThumbprint(ctx, clientCert.Thumbprint) ??
                                  ServerStore.Cluster.GetLocalStateByThumbprint(ctx, clientCert.Thumbprint);

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
                                PublicKeyPinningHash = Server.Certificate.Certificate.GetPublicKeyPinningHash(),
                                NotAfter = Server.Certificate.Certificate.NotAfter
                            };

                            certificate = ctx.ReadObject(serverCertDef.ToJson(), "Server/Certificate/Definition");
                        }
                    }
                    else if (wellKnown != null && wellKnown.Contains(clientCert.Thumbprint, StringComparer.OrdinalIgnoreCase))
                    {
                        var wellKnownCertDef = new CertificateDefinition
                        {
                            Name = "Well Known Admin Certificate",
                            Permissions = new Dictionary<string, DatabaseAccess>(),
                            SecurityClearance = SecurityClearance.ClusterAdmin,
                            Thumbprint = clientCert.Thumbprint,
                            PublicKeyPinningHash = clientCert.GetPublicKeyPinningHash()
                        };
                        certificate = ctx.ReadObject(wellKnownCertDef.ToJson(), "WellKnown/Certificate/Definition");
                    }
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, ResponseBodyStream()))
                {
                    writer.WriteObject(certificate);
                }
            }
        }

        [RavenAction("/admin/certificates/edit", "POST", AuthorizationStatus.Operator)]
        public async Task Edit()
        {
            await ServerStore.EnsureNotPassiveAsync();

            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var clientCert = feature?.Certificate;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var certificateJson = await ctx.ReadForDiskAsync(RequestBodyStream(), "edit-certificate"))
            {
                var newCertificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                ValidateCertificateDefinition(newCertificate, ServerStore);

                CertificateDefinition existingCertificate;
                using (ctx.OpenWriteTransaction())
                {
                    var certificate = ServerStore.Cluster.GetCertificateByThumbprint(ctx, newCertificate.Thumbprint);
                    if (certificate == null)
                        throw new InvalidOperationException($"Cannot edit permissions for certificate with thumbprint '{newCertificate.Thumbprint}'. It doesn't exist in the cluster.");

                    existingCertificate = JsonDeserializationServer.CertificateDefinition(certificate);

                    if ((existingCertificate.SecurityClearance == SecurityClearance.ClusterAdmin || existingCertificate.SecurityClearance == SecurityClearance.ClusterNode) && IsClusterAdmin() == false)
                    {
                        var clientCertDef = ReadCertificateFromCluster(ctx, clientCert?.Thumbprint);
                        throw new InvalidOperationException($"Cannot edit the certificate '{existingCertificate.Name}'. It has '{existingCertificate.SecurityClearance}' security clearance while the current client certificate being used has a lower clearance: {clientCertDef.SecurityClearance}");
                    }

                    if ((newCertificate.SecurityClearance == SecurityClearance.ClusterAdmin || newCertificate.SecurityClearance == SecurityClearance.ClusterNode) && IsClusterAdmin() == false)
                    {
                        var clientCertDef = ReadCertificateFromCluster(ctx, clientCert?.Thumbprint);
                        throw new InvalidOperationException($"Cannot edit security clearance to '{newCertificate.SecurityClearance}' for certificate '{existingCertificate.Name}'. Only a 'Cluster Admin' can do that and your current client certificate has a lower clearance: {clientCertDef.SecurityClearance}");
                    }

                    ServerStore.Cluster.DeleteLocalState(ctx, newCertificate.Thumbprint);
                }

                var putResult = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(newCertificate.Thumbprint,
                    new CertificateDefinition
                    {
                        Name = newCertificate.Name,
                        Certificate = existingCertificate.Certificate,
                        Permissions = newCertificate.Permissions,
                        SecurityClearance = newCertificate.SecurityClearance,
                        Thumbprint = existingCertificate.Thumbprint,
                        PublicKeyPinningHash = existingCertificate.PublicKeyPinningHash,
                        NotAfter = existingCertificate.NotAfter
                    }, GetRaftRequestIdFromQuery()));
                await ServerStore.Cluster.WaitForIndexNotification(putResult.Index);

                NoContentStatus(HttpStatusCode.Created);
            }
        }

        [RavenAction("/admin/certificates/export", "GET", AuthorizationStatus.Operator)]
        public async Task GetClusterCertificates()
        {
            if (Server.Certificate.Certificate == null)
                return;

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
                            allItems = ClusterStateMachine.GetAllCertificatesFromCluster(context, 0, long.MaxValue).ToList();
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

            await HttpContext.Response.Body.WriteAsync(pfx, 0, pfx.Length);
        }

        [RavenAction("/admin/certificates/mode", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task Mode()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(ctx, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("SetupMode");
                    writer.WriteString(ServerStore.Configuration.Core.SetupMode.ToString());
                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/admin/certificates/cluster-domains", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task ClusterDomains()
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

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
        public async Task ReplacementStatus()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
        }

        [RavenAction("/admin/certificates/letsencrypt/renewal-date", "GET", AuthorizationStatus.ClusterAdmin)]
        public async Task RenewalDate()
        {
            if (ServerStore.Configuration.Core.SetupMode != SetupMode.LetsEncrypt)
                throw new InvalidOperationException("This server wasn't set up using the Let's Encrypt setup mode.");

            if (Server.Certificate.Certificate == null)
                throw new InvalidOperationException("The server certificate is not loaded.");

            var (_, renewalDate) = Server.CalculateRenewalDate(Server.Certificate, false);

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("EstimatedRenewal");
                writer.WriteDateTime(renewalDate, true);
                writer.WriteEndObject();
            }
        }

        [RavenAction("/admin/certificates/letsencrypt/force-renew", "POST", AuthorizationStatus.ClusterAdmin, CorsMode = CorsMode.Cluster)]
        public async Task ForceRenew()
        {
            if (ServerStore.IsLeader())
            {
                if (ServerStore.Configuration.Core.SetupMode != SetupMode.LetsEncrypt)
                    throw new InvalidOperationException("Cannot force renew for this server certificate. This server wasn't set up using the Let's Encrypt setup mode.");

                if (Server.Certificate.Certificate == null)
                    throw new InvalidOperationException("Cannot force renew this Let's Encrypt server certificate. The server certificate is not loaded.");

                try
                {
                    var success = Server.RefreshClusterCertificate(true, GetRaftRequestIdFromQuery());
                    using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName(nameof(ForceRenewResult.Success));
                        writer.WriteBool(success);
                        writer.WriteEndObject();
                    }

                    return;
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Failed to force renew the Let's Encrypt server certificate for domain: {Server.Certificate.Certificate.GetNameInfo(X509NameType.SimpleName, false)}", e);
                }
            }

            RedirectToLeader();
        }

        [RavenAction("/admin/certificates/refresh", "POST", AuthorizationStatus.ClusterAdmin, CorsMode = CorsMode.Cluster)]
        public Task TriggerCertificateRefresh()
        {
            if (ServerStore.IsLeader() || ServerStore.Configuration.Core.SetupMode != SetupMode.LetsEncrypt)
            {
                // What we do here is trigger the refresh cycle which normally happens once an hour.

                // The difference between this and /admin/certificates/letsencrypt/force-renew is that here we also allow it for non-LetsEncrypt setups
                // in which case we'll check if the certificate changed on disk (or via executable) and if so we'll update it immediately on the local node (only)

                var replaceImmediately = GetBoolValueQueryString("replaceImmediately", required: false) ?? false;

                if (Server.Certificate.Certificate == null)
                    throw new InvalidOperationException("Failed to trigger a certificate refresh cycle. The server certificate is not loaded.");

                try
                {
                    Server.RefreshClusterCertificate(replaceImmediately, GetRaftRequestIdFromQuery());
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

        [RavenAction("/admin/certificates/replace-cluster-cert", "POST", AuthorizationStatus.ClusterAdmin, CorsMode = CorsMode.Cluster)]
        public async Task ReplaceClusterCert()
        {
            if (ServerStore.IsLeader())
            {
                var replaceImmediately = GetBoolValueQueryString("replaceImmediately", required: false) ?? false;

                await ServerStore.EnsureNotPassiveAsync();
                using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (var certificateJson = await ctx.ReadForDiskAsync(RequestBodyStream(), "replace-cluster-cert"))
                {
                    try
                    {
                        var certificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                        if (string.IsNullOrWhiteSpace(certificate.Certificate))
                            throw new ArgumentException($"{nameof(certificate.Certificate)} is a required field in the certificate definition.");

                        var certBytes = Convert.FromBase64String(certificate.Certificate);

                        // Load the password protected certificate and export it without a password, to send it through the cluster.
                        if (string.IsNullOrWhiteSpace(certificate.Password) == false)
                        {
                            try
                            {
                                var cert = new X509Certificate2Collection();
                                cert.Import(certBytes, certificate.Password, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
                                // Exporting with the private key, but without the password
                                certBytes = cert.Export(X509ContentType.Pkcs12);
                                certificate.Certificate = Convert.ToBase64String(certBytes);
                            }
                            catch (Exception e)
                            {
                                throw new ArgumentException("Failed to load the password protected certificate and export it back. Is the password correct?", e);
                            }
                        }

                        // Ensure we'll be able to load the certificate
                        try
                        {
                            var _ = new X509Certificate2(certBytes, (string)null, X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
                        }
                        catch (Exception e)
                        {
                            throw new ArgumentException("Unable to load the provided certificate.", e);
                        }

                        if (IsClusterAdmin() == false)
                            throw new InvalidOperationException("Cannot replace the server certificate. Only a ClusterAdmin can do this.");

                        var timeoutTask = TimeoutManager.WaitFor(TimeSpan.FromSeconds(60), ServerStore.ServerShutdown);

                        var replicationTask = Server.StartCertificateReplicationAsync(certBytes, replaceImmediately, GetRaftRequestIdFromQuery());

                        await Task.WhenAny(replicationTask, timeoutTask);
                        if (replicationTask.IsCompleted == false)
                            throw new TimeoutException("Timeout when trying to replace the server certificate.");
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException("Failed to replace the server certificate.", e);
                    }
                }

                NoContentStatus(HttpStatusCode.Created);
                return;
            }

            RedirectToLeader();
        }

        [RavenAction("/admin/certificates/local-state", "GET", AuthorizationStatus.Operator)]
        public async Task GetLocalState()
        {
            var includeSecondary = GetBoolValueQueryString("secondary", required: false) ?? false;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var certificateList = new Dictionary<string, BlittableJsonReaderObject>();

                try
                {
                    var localCertKeys = ServerStore.Cluster.GetCertificateThumbprintsFromLocalState(context).ToList();

                    foreach (var localCertKey in localCertKeys)
                    {
                        var localCertificate = ServerStore.Cluster.GetLocalStateByThumbprint(context, localCertKey);
                        if (localCertificate == null)
                            continue;

                        var def = JsonDeserializationServer.CertificateDefinition(localCertificate);

                        if (includeSecondary || string.IsNullOrEmpty(def.CollectionPrimaryKey))
                            certificateList.TryAdd(localCertKey, localCertificate);
                        else
                            localCertificate.Dispose();
                    }

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray(context, "Results", certificateList.ToArray(), (w, c, cert) => c.Write(w, cert.Value));
                        writer.WriteEndObject();
                    }
                }
                finally
                {
                    foreach (var cert in certificateList)
                        cert.Value?.Dispose();
                }
            }
        }

        [RavenAction("/admin/certificates/local-state", "DELETE", AuthorizationStatus.ClusterAdmin)]
        public Task LocalStateDelete()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                List<string> localStateKeys;
                using (ctx.OpenReadTransaction())
                {
                    localStateKeys = ServerStore.Cluster.GetCertificateThumbprintsFromLocalState(ctx).ToList();
                }

                // Delete from local state
                using (var tx = ctx.OpenWriteTransaction())
                {
                    ServerStore.Cluster.DeleteLocalState(ctx, localStateKeys);
                    tx.Commit();
                }
            }

            return NoContent();
        }

        [RavenAction("/admin/certificates/local-state/apply", "POST", AuthorizationStatus.ClusterAdmin)]
        public Task LocalStateApply()
        {
            if (ServerStore.CurrentRachisState == RachisState.Passive)
                throw new AuthorizationException("RavenDB is in passive state. Cannot apply certificates to the cluster.");

            var raftRequestId = GetRaftRequestIdFromQuery();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                List<string> localStateKeys;
                using (ctx.OpenReadTransaction())
                {
                    localStateKeys = ServerStore.Cluster.GetCertificateThumbprintsFromLocalState(ctx).ToList();
                    foreach (var localStateKey in localStateKeys)
                    {
                        // if there are trusted certificates in the local state, we will register them in the cluster now
                        using (var localCertificate = ServerStore.Cluster.GetLocalStateByThumbprint(ctx, localStateKey))
                        {
                            var certificateDefinition = JsonDeserializationServer.CertificateDefinition(localCertificate);

                            // In the beginning of 4.0 we had the server certificate stored together with all the other certs (in the local state and in the cluster).
                            // If it's the case now, we make sure not to transfer it to the cluster.
                            if (certificateDefinition.Thumbprint == ServerStore.Server.Certificate.Certificate.Thumbprint)
                                continue;

                            ServerStore.PutValueInClusterAsync(new PutCertificateCommand(localStateKey, certificateDefinition, $"{raftRequestId}/{localStateKey}"))
                                .Wait(ServerStore.ServerShutdown);
                        }
                    }
                }

                // Delete from local state
                using (var tx = ctx.OpenWriteTransaction())
                {
                    ServerStore.Cluster.DeleteLocalState(ctx, localStateKeys);
                    tx.Commit();
                }
            }

            return NoContent();
        }

        public static void ValidateCertificateDefinition(CertificateDefinition certificate, ServerStore serverStore)
        {
            if (string.IsNullOrWhiteSpace(certificate.Name))
                throw new ArgumentException($"{nameof(certificate.Name)} is a required field in the certificate definition");

            if (certificate.NotAfter.HasValue && certificate.NotAfter <= DateTime.UtcNow.Date)
                throw new ArgumentException("The certificate expiration date must be in the future.");

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

                switch (kvp.Value)
                {
                    case DatabaseAccess.ReadWrite:
                    case DatabaseAccess.Admin:
                    case DatabaseAccess.Read:
                        break;

                    default:
                        throw new ArgumentException($"Error in permissions in the certificate definition, invalid access {kvp.Value} for database {kvp.Key}");
                }
            }
        }
    }
}
