using System;
using System.Security.Cryptography;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders.Admin
{
    public class AdminOAuthCertificateResponder : AbstractRequestResponder
    {
        public override string UrlPattern
        {
            get
            {
                return "^/admin/generate-oauth-certificate";
            }
        }

        public override string[] SupportedVerbs
        {
            get
            {
                return new[] { "GET" };
            }
        }

        public override void Respond(IHttpContext context)
        {
            string certificate;
            using (var rsa = new RSACryptoServiceProvider())
                certificate = Convert.ToBase64String(rsa.ExportCspBlob(true));
            context.Response.AddHeader("content-disposition", "attachment; filename=oauth-certificate.txt");
            context.Response.ContentType = "application/octet-stream";
            context.Write(certificate);
        }
    }
}
