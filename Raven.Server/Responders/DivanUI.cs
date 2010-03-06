using System.IO;
using System.Net;

namespace Raven.Server.Responders
{
    public class DivanUI : RequestResponder
    {
        public string DivanPath
        {
            get { return Settings.WebDir; }
        }

        public override string UrlPattern
        {
            get { return "^/divan/"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET"}; }
        }

        public override void Respond(HttpListenerContext context)
        {
            var docPath = context.Request.Url.LocalPath.Replace("/divan/", "");
            var filePath = Path.Combine(DivanPath, docPath);
            var bytes = File.ReadAllBytes(filePath);
            context.Response.OutputStream.Write(bytes, 0, bytes.Length);
            context.Response.OutputStream.Flush();
        }
    }
}