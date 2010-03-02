using System.Net;

namespace Raven.Server.Responders
{
    public class Document : RequestResponder
    {
        public override string UrlPattern
        {
            get { return @"/docs/([\w\d_-]+)"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "GET", "DELETE", "PUT" }; }
        }

        public override void Respond(HttpListenerContext context)
        {
            var match = urlMatcher.Match(context.Request.Url.LocalPath);
            var docId = match.Groups[1].Value;
            switch (context.Request.HttpMethod)
            {
                case "GET":
                    context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
                    var doc = Database.Get(docId);
                    if(doc==null)
                    {
                        context.SetStatusToNotFound();
                        return;
                    }
                    context.WriteData(doc.Data, doc.Metadata);
                    break;
                case "DELETE":
                    Database.Delete(docId);
                    context.SetStatusToDeleted();
                    break;
                case "PUT":
                    Put(context, docId);
                    break;
            }
        }

        private void Put(HttpListenerContext context, string docId)
        {
            var json = context.ReadJson();
            context.SetStatusToCreated("/docs/" + docId);
            context.WriteJson(new { id = Database.Put(docId, json, 
                                                      context.Request.Headers.FilterHeaders()
                                  ) });
        }
    }
}