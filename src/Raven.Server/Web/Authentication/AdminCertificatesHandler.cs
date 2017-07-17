using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
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
                
                ValidateCertificate(certificateJson, out var friendlyName, out var password, out var serverAdmin, out var permissions);

                // this creates a client certificate which is signed by the current server certificate
                var certificate = CertificateUtils.CreateSelfSignedClientCertificate(friendlyName, Server.ServerCertificateHolder);

                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + certificate.Thumbprint,
                    new CertificateDefinition
                    {
                        // this does not include the private key, that is only for the client
                        Certificate = Convert.ToBase64String(certificate.Export(X509ContentType.Cert)),
                        Permissions = new HashSet<string>(permissions.Select(x => x?.ToString())),
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
        
        [RavenAction("/admin/certificates", "PUT", "/admin/certificates", RequiredAuthorization = AuthorizationStatus.ServerAdmin)]
        public async Task Put()
        {
            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node
            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var certificateJson = ctx.ReadForDisk(RequestBodyStream(), "put-certificate"))
            {
                ValidatePermissions(certificateJson, out _, out _);

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
                ServerStore.Cluster.DeleteLocalState(ctx, Constants.Certificates.Prefix + thumbprint);

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
                        foreach (var cert in certificates)
                            cert.Item2?.Dispose();
                    }
                }
            }

            return Task.CompletedTask;
        }

        private static void ValidateCertificate(
            BlittableJsonReaderObject certificateJson, 
            out string friendlyName, 
            out string password, 
            out bool serverAdmin, 
            out BlittableJsonReaderArray permissions)
        {
            if (certificateJson.TryGet("Name", out friendlyName) == false)
                throw new ArgumentException("'Name' is a required field when generating a new certificate");

            if (string.IsNullOrWhiteSpace(friendlyName))
                throw new ArgumentException("'Name' cannot be empty when generating a new certificate");
            
            certificateJson.TryGet("Password", out password); //can be null

            ValidatePermissions(certificateJson, out permissions, out serverAdmin);
        }

        private static void ValidatePermissions(BlittableJsonReaderObject certificateJson, out BlittableJsonReaderArray permissions, out bool serverAdmin)
        {
            certificateJson.TryGet("ServerAdmin", out serverAdmin); //can be null, default is false

            if (certificateJson.TryGet("Permissions", out permissions) == false)
                throw new ArgumentException("'Permissions' is a required field when generating a new certificate");

            const string validDbNameChars = @"([A-Za-z0-9_\-\.]+)";

            if (permissions.Items.Count() > 0 && serverAdmin == true)
                throw new ArgumentException("Certificate contains non empty 'Permissions' but 'serverAdmin' is set to true. Server Admin has access to everything so 'permissions' must be empty.");

            foreach (LazyStringValue dbName in permissions.Items)
            {
                if (string.IsNullOrWhiteSpace(dbName))
                    throw new ArgumentNullException(nameof(permissions));

                if (dbName.Length > Constants.Documents.MaxDatabaseNameLength)
                    throw new InvalidOperationException($"Database name '{dbName}' exceeds {Constants.Documents.MaxDatabaseNameLength} characters.");

                var result = Regex.Matches(dbName, validDbNameChars);
                if (result.Count == 0 || result[0].Value != dbName)
                {
                    throw new InvalidOperationException(
                        "Database name can only contain A-Z, a-z, \"_\", \".\" or \"-\" chars but was: '" + dbName + "'");
                }
            }
        }
    }
}
