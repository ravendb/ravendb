using System;
using System.Linq;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.Authentication
{
    public class AdminCertificatesHandler : AdminRequestHandler
    {

        [RavenAction("/admin/certificates", "POST", "/admin/certificates?name={certificate-name:string}")]
        public async Task Generate()
        {
            // generate self signed certificate (from the certificate of the server)
            // expiration (5 years?), friendly name from the client

            //X509Certificate2 f = new X509Certificate2();
        }

        [RavenAction("/admin/certificates", "PUT", "/admin/certificates?name={certificate-name:string}")]
        public async Task Put()
        {
            // {'Certificate': 'base64', 'Permissions': [] }

            X509Certificate2 f = new X509Certificate2();
            if (f.HasPrivateKey)
            {
                // error here, we don't accept it
            }


            // Do we want a name for a certificate?
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            // one of the first admin action is to create a certificate, so let
            // us also use that to indicate that we are the seed node
            ServerStore.EnsureNotPassive();
            //TODO: validation
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var certificateJson = ctx.ReadForDisk(RequestBodyStream(), name);
                
                var certificate = JsonDeserializationServer.CertificateDefinition(certificateJson);
                var res = await ServerStore.PutValueInClusterAsync(new PutCertificateCommand(Constants.Certificates.Prefix + f.Thumbprint, certificate));
                await ServerStore.Cluster.WaitForIndexNotification(res.Etag);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
            }
        }

        [RavenAction("/admin/certificates", "DELETE", "/admin/certificates?name={certificate-name:string}")]
        public async Task Delete()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

             using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            {
                var res = await ServerStore.DeleteValueInClusterAsync(Constants.Certificates.Prefix + name);
                await ServerStore.Cluster.WaitForIndexNotification(res.Etag);
             
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NoContent;
            }
        }

    
        [RavenAction("/admin/certificates", "GET", "/admin/certificates")]
        public Task GetAll()
        {
            var name = GetStringQueryString("name", required: false);

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
                    if (string.IsNullOrEmpty(name))
                        certificates = ServerStore.Cluster.ItemsStartingWith(context, Constants.Certificates.Prefix, start, pageSize)
                            .ToArray();
                    else
                    {
                        var key = Constants.Certificates.Prefix + name;
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
                            // Do we want to display the names as well?
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
