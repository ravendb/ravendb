using System;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http.Features.Authentication;
using Raven.Client;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Json;

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
                var stream = TryGetRequestFormStream("Options") ?? RequestBodyStream();

                var certificateJson = ctx.ReadForDisk(stream, "certificate-generation");

                var certificate = JsonDeserializationServer.CertificateDefinition(certificateJson);

                ValidateCertificate(certificate, ServerStore);

                if (certificate.SecurityClearance == SecurityClearance.ClusterAdmin && IsClusterAdmin() == false)
                {
                    var clientCert = (HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection)?.Certificate;
                    throw new InvalidOperationException($"Cannot generate the certificate '{certificate.Name}' with 'Cluster Admin' security clearance because the current client certificate being used has a lower clearance: {clientCert}");
                }

                if (Server.ClusterCertificateHolder?.Certificate == null)
                    throw new InvalidOperationException($"Cannot generate the client certificate '{certificate.Name}' becuase the server certificate is not loaded. " +
                                                        $"You can supply a server certificate by using the following configuration keys: " +
                                                        $"'{RavenConfiguration.GetKey(x => x.Security.CertificatePath)}'/'{RavenConfiguration.GetKey(x => x.Security.CertificateExec)}'/" +
                                                        $"'{RavenConfiguration.GetKey(x => x.Security.ClusterCertificatePath)}'/'{RavenConfiguration.GetKey(x => x.Security.ClusterCertificateExec)}'. " +
                                                        $"For a more detailed explanation please read about authentication and certificates in the RavenDB documentation.");

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

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(certificate.Name) + ".pfx";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "binary/octet-stream";
                var pfx = selfSignedCertificate.Export(X509ContentType.Pfx, certificate.Password);
                HttpContext.Response.Body.Write(pfx, 0, pfx.Length);
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

                ValidateCertificate(certificate, ServerStore);

                if (certificate.SecurityClearance == SecurityClearance.ClusterAdmin && IsClusterAdmin() == false)
                {
                    var clientCert = (HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection)?.Certificate;
                    throw new InvalidOperationException($"Cannot save the certificate '{certificate.Name}' with 'Cluster Admin' security clearance because the current client certificate being used has a lower clearance: {clientCert}");
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
                var x509Certificate = new X509Certificate2(certBytes);
                if (x509Certificate.HasPrivateKey)
                {
                    // avoid storing the private key
                    certificate.Certificate = Convert.ToBase64String(x509Certificate.Export(X509ContentType.Cert));
                }
                certificate.Thumbprint = x509Certificate.Thumbprint;

                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + x509Certificate.Thumbprint, certificate));
                await ServerStore.Cluster.WaitForIndexNotification(res.Index);

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

            if (clientCert != null && clientCert.Thumbprint.Equals(thumbprint))
                throw new InvalidOperationException($"Cannot delete {clientCert.FriendlyName} becuase it's the current client certificate being used");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var key = Constants.Certificates.Prefix + thumbprint;
                using (ctx.OpenWriteTransaction())
                {
                    var certificate = ServerStore.Cluster.Read(ctx, key);

                    var definition = JsonDeserializationServer.CertificateDefinition(certificate);

                    if (definition?.SecurityClearance == SecurityClearance.ClusterAdmin && IsClusterAdmin() == false)
                        throw new InvalidOperationException(
                            $"Cannot delete the certificate '{definition?.Name}' with 'Cluster Admin' security clearance because the current client certificate being used has a lower clearance: {clientCert}");

                    ServerStore.Cluster.DeleteLocalState(ctx, key);
                }

                var res = await ServerStore.SendToLeaderAsync(new DeleteCertificateFromClusterCommand
                {
                    Name = Constants.Certificates.Prefix + thumbprint
                });
                await ServerStore.Cluster.WaitForIndexNotification(res.Index);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            }
        }

        [RavenAction("/admin/certificates", "GET", AuthorizationStatus.Operator)]
        public Task GetAll()
        {
            var thumbprint = GetStringQueryString("thumbprint", required: false);

            var start = GetStart();
            var pageSize = GetPageSize();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                Tuple<string, BlittableJsonReaderObject>[] certificates = null;
                try
                {
                    if (string.IsNullOrEmpty(thumbprint))
                        certificates = ServerStore.Cluster.ItemsStartingWith(context, Constants.Certificates.Prefix, start, pageSize)
                            .ToArray();
                    else
                    {
                        var key = Constants.Certificates.Prefix + thumbprint;
                        var certificate = ServerStore.Cluster.Read(context, key);
                        if (certificate == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return Task.CompletedTask;
                        }

                        certificates = new[]
                        {
                            Tuple.Create(key, certificate)
                        };
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WriteArray(context, "Results", certificates, (w, c, cert) =>
                        {
                            c.Write(w, cert.Item2);
                        });
                        writer.WriteEndObject();
                    }

                }
                finally
                {
                    if (certificates != null)
                    {
                        foreach (var cert in certificates)
                            cert.Item2?.Dispose();
                    }
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/admin/edit-certificate", "Post", AuthorizationStatus.Operator)]
        public async Task EditPermissions()
        {
            ServerStore.EnsureNotPassive();

            var feature = HttpContext.Features.Get<IHttpAuthenticationFeature>() as RavenServer.AuthenticateConnection;
            var clientCert = feature?.Certificate;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var certificateJson = ctx.ReadForDisk(RequestBodyStream(), "edit-certificate-permissions"))
            {
                var newPermissions = JsonDeserializationServer.CertificateDefinition(certificateJson);

                ValidatePermissions(newPermissions, ServerStore);

                var key = Constants.Certificates.Prefix + newPermissions.Thumbprint;

                CertificateDefinition existingCertificate;
                using (ctx.OpenWriteTransaction())
                {
                    var certificate = ServerStore.Cluster.Read(ctx, key);
                    if (certificate == null)
                        throw new InvalidOperationException($"Cannot edit permissions for certificate with thumbprint '{newPermissions.Thumbprint}'. It doesn't exist in the cluster.");

                    existingCertificate = JsonDeserializationServer.CertificateDefinition(certificate);

                    if (existingCertificate.SecurityClearance == SecurityClearance.ClusterAdmin && IsClusterAdmin() == false)
                        throw new InvalidOperationException($"Cannot edit the certificate '{existingCertificate.Name}'. It has 'Cluster Admin' security clearance while the current client certificate being used has a lower clearance: {clientCert}");

                    if (newPermissions.SecurityClearance == SecurityClearance.ClusterAdmin && IsClusterAdmin() == false)
                        throw new InvalidOperationException($"Cannot edit security clearance to 'Cluster Admin' for certificate '{existingCertificate.Name}'. Only a 'Cluster Admin' can do that and your current client certificate has a lower clearance: {clientCert}");

                    ServerStore.Cluster.DeleteLocalState(ctx, key);
                }

                var deleteResult = await ServerStore.SendToLeaderAsync(new DeleteCertificateFromClusterCommand
                {
                    Name = Constants.Certificates.Prefix + existingCertificate.Thumbprint
                });
                await ServerStore.Cluster.WaitForIndexNotification(deleteResult.Index);

                var putResult = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + newPermissions.Thumbprint,
                    new CertificateDefinition
                    {
                        Name = existingCertificate.Name,
                        Certificate = existingCertificate.Certificate,
                        Permissions = newPermissions.Permissions,
                        SecurityClearance = newPermissions.SecurityClearance,
                        Thumbprint = existingCertificate.Thumbprint
                    }));
                await ServerStore.Cluster.WaitForIndexNotification(putResult.Index);

                NoContentStatus();
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
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
