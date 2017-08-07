using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.Authentication
{
    public class AdminCertificatesHandler : RequestHandler
    {

        [RavenAction("/admin/certificates", "POST", AuthorizationStatus.ServerAdmin)]
        public async Task Generate()
        {
            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node
            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var certificateJson = ctx.ReadForDisk(RequestBodyStream(), "certificate-generation");
                
                ValidateCertificate(certificateJson, out var friendlyName, out var password, out var serverAdmin, out Dictionary<string, DatabaseAccess> permissions);
                
                // this creates a client certificate which is signed by the current server certificate
                var certificate = CertificateUtils.CreateSelfSignedClientCertificate(friendlyName, Server.ServerCertificateHolder);
                
                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + certificate.Thumbprint,
                    new CertificateDefinition
                    {
                        // this does not include the private key, that is only for the client
                        Certificate = Convert.ToBase64String(certificate.Export(X509ContentType.Cert)),
                        Permissions = permissions,
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
        
        [RavenAction("/admin/certificates", "PUT", AuthorizationStatus.ServerAdmin)]
        public async Task Put()
        {
            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node
            ServerStore.EnsureNotPassive();
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (var certificateJson = ctx.ReadForDisk(RequestBodyStream(), "put-certificate"))
            {
                ValidatePermissions(certificateJson, out var permissions, out var serverAdmin);

                if(certificateJson.TryGet("Certificate", out string certificate) == false)
                    throw new ArgumentException("'Certificate' is a mandatory property");

                var certificateDefinition = new CertificateDefinition()
                {
                    Certificate = certificate,
                    Permissions = permissions,
                    ServerAdmin = serverAdmin
                };

                byte[] certBytes;
                try
                {
                    certBytes = Convert.FromBase64String(certificate);
                }
                catch (Exception e)
                {
                    throw new ArgumentException("Unable to parse the 'Certificate' property, expected Base64 value", e);
                }
                var x509Certificate = new X509Certificate2(certBytes);
                if (x509Certificate.HasPrivateKey)
                {
                    // avoid storing the private key
                    certificateDefinition.Certificate = Convert.ToBase64String(x509Certificate.Export(X509ContentType.Cert));
                }
                certificateDefinition.Thumbprint = x509Certificate.Thumbprint;
                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + x509Certificate.Thumbprint, certificateDefinition));
                await ServerStore.Cluster.WaitForIndexNotification(res.Etag);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        [RavenAction("/admin/certificates", "DELETE", AuthorizationStatus.ServerAdmin)]
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
    
        [RavenAction("/admin/certificates", "GET", AuthorizationStatus.ServerAdmin)]
        public Task GetAll()
        {
            var thumbprint = GetStringQueryString("thumbprint", required: false);

            var start = GetStart();
            var pageSize = GetPageSize();

            // TODO: cert.tostring(), expiration, permissions

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

        private static void ValidateCertificate(
            BlittableJsonReaderObject certificateJson, 
            out string friendlyName, 
            out string password, 
            out bool serverAdmin, 
            out Dictionary<string, DatabaseAccess> permissions)
        {
            if (certificateJson.TryGet("Name", out friendlyName) == false)
                throw new ArgumentException("'Name' is a required field when generating a new certificate");

            if (string.IsNullOrWhiteSpace(friendlyName))
                throw new ArgumentException("'Name' cannot be empty when generating a new certificate");
            
            certificateJson.TryGet("Password", out password); //can be null

            ValidatePermissions(certificateJson, out permissions, out serverAdmin);
        }

        private static void ValidatePermissions(BlittableJsonReaderObject certificateJson, out Dictionary<string, DatabaseAccess> permissions, out bool serverAdmin)
        {
            certificateJson.TryGet("ServerAdmin", out serverAdmin); //can be null, default is false

            if (certificateJson.TryGet("Permissions", out BlittableJsonReaderArray permissionsJson) == false)
                throw new ArgumentException("'Permissions' is a required field when generating a new certificate");

            const string validDbNameChars = @"([A-Za-z0-9_\-\.]+)";
            
            permissions = new Dictionary<string, DatabaseAccess>();
            foreach (BlittableJsonReaderObject kvp in permissionsJson.Items)
            {
                if (kvp.TryGet("Database", out string dbName) == false)
                    throw new ArgumentException("'Database' is a required field in 'Permissions' when generating a new certificate");
                if (kvp.TryGet("Access", out string accessString) == false)
                    throw new ArgumentException("'Access' is a required field in 'Permissions' when generating a new certificate");
                if (accessString != nameof(DatabaseAccess.ReadWrite) && accessString != nameof(DatabaseAccess.Admin))
                    throw new ArgumentException($"Invalid access {accessString} for database {dbName} when generating a new certificate");

                permissions.Add(dbName, accessString == nameof(DatabaseAccess.ReadWrite) ? DatabaseAccess.ReadWrite : DatabaseAccess.Admin);

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
