using System;
using System.IO;
using System.Net;

namespace Rhino.DivanDB.Server.Responders
{
    public class DivanUserInterfaceResponser : RequestResponder
    {
        public const string DivanPath = @"C:\Work\divandb\Rhino.DivanDB.Server\WebUI\";

        public override string UrlPattern
        {
            get { return "/divan/"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET"}; }
        }

        public override void Respond(HttpListenerContext context)
        {
            var docPath = context.Request.Url.LocalPath.Replace("/divan/", "");
            var filePath = Path.Combine(DivanPath, docPath);
            var bytes = File.ReadAllBytes(filePath);
            context.Response.OutputStream.Write(bytes,0,bytes.Length);
            context.Response.OutputStream.Flush();  
        }
    }
}