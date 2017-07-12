using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Server.Operations.Certificates;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Web.Authentication
{
    public class AdminCertificatesHandler : RequestHandler
    {

        [RavenAction("/admin/certificates", "POST", "/admin/certificates", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
        public async Task Generate()
        {
            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node
            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var certificateJson = ctx.ReadForDisk(RequestBodyStream(), "certificate-generation");

                if (certificateJson.TryGet("Name", out string friendlyName) == false)
                    throw new ArgumentException("'Name' is a required field when generating a new certificate");

                certificateJson.TryGet("Password", out string password); // okay to be null
                certificateJson.TryGet("ServerAdmin", out bool serverAdmin); // okay to be null, default to false

                if (certificateJson.TryGet("Permissions", out BlittableJsonReaderArray permissions) == false)
                    throw new ArgumentException("'Permissions' is a required field when generating a new certificate");

                var certificate = CertificateUtils.CreateSelfSignedClientCertificate(friendlyName, Server.ServerCertificateHolder);

                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + certificate.Thumbprint,
                    new CertificateDefinition
                    {
                        // this does not include the private key, that is only for the client
                        Certificate = Convert.ToBase64String(certificate.Export(X509ContentType.Cert)),
                        Databases = new HashSet<string>(permissions.OfType<string>()),
                        ServerAdmin = serverAdmin,
                        Thumbprint = certificate.Thumbprint
                    }));
                await ServerStore.Cluster.WaitForIndexNotification(res.Etag);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                var contentDisposition = "attachment; filename=" + Uri.EscapeDataString(friendlyName) + ".pfx";
                HttpContext.Response.Headers["Content-Disposition"] = contentDisposition;
                HttpContext.Response.ContentType = "binary/octet-stream";
                var pfx = certificate.Export(X509ContentType.Pfx, password);
                HttpContext.Response.Body.Write(pfx, 0, pfx.Length);
            }
        }

        [RavenAction("/admin/certificates", "PUT", "/admin/certificates?name={certificate-name:string}", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
        public async Task Put()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node
            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var certificateJson = ctx.ReadForDisk(RequestBodyStream(), name);
                
                var certificateDefinition = JsonDeserializationServer.CertificateDefinition(certificateJson);

                var certificate = new X509Certificate2(Convert.FromBase64String(certificateDefinition.Certificate));
                if (certificate.HasPrivateKey)
                {
                    // avoid storing the private key
                    certificateDefinition.Certificate = Convert.ToBase64String(certificate.Export(X509ContentType.Cert));
                }
                certificateDefinition.Thumbprint = certificate.Thumbprint;
                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + certificate.Thumbprint, certificateDefinition));
                await ServerStore.Cluster.WaitForIndexNotification(res.Etag);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        [RavenAction("/admin/certificates", "DELETE", "/admin/certificates?thumbprint={certificate-thumbprint:string}", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
        public async Task Delete()
        {
            var thumbprint = GetQueryStringValueAndAssertIfSingleAndNotEmpty("thumbprint");

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var res = await ServerStore.DeleteValueInClusterAsync(Constants.Certificates.Prefix + thumbprint);
                await ServerStore.Cluster.WaitForIndexNotification(res.Etag);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            }
        }
    
        [RavenAction("/admin/certificates", "GET", "/admin/certificates", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
        public Task GetAll()
        {
            var thumbprint = GetStringQueryString("thumbprint", required: false);

            var start = GetStart();
            var pageSize = GetPageSize();

            // TODO: cert.tostring(), expiration, permissions

            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
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
                        foreach(var cert in certificates)
                            cert.Item2?.Dispose();
                    }
                }
            }

            return Task.CompletedTask;
        }
    }
}
