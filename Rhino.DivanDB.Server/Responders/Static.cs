using System.Net;

namespace Rhino.DivanDB.Server.Responders
{
    public class Static : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/static/(.+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET", "PUT", "DELETE"}; }
        }

        public override void Respond(HttpListenerContext context)
        {
            var match = urlMatcher.Match(context.Request.Url.LocalPath);
            var filename = match.Groups[1].Value;
            switch (context.Request.HttpMethod)
            {
                case "GET":
                    var attachmentAndHeaders = Database.GetStatic(filename);
                    if(attachmentAndHeaders == null)
                    {
                        context.SetStatusToNotFound();
                        return;
                    }
                    context.WriteData(attachmentAndHeaders.Data, attachmentAndHeaders.Metadata);
                    break;
                case "PUT":
                    Database.PutStatic(filename, context.Request.InputStream.ReadData(), context.Request.Headers.FilterHeaders());
                    context.SetStatusToCreated("/static/"+filename);
                    break;
                case "DELETE":
                    Database.DeleteStatic(filename);
                    context.SetStatusToDeleted();
                    break;
            }
        }
    }
}