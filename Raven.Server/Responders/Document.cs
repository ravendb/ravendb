using System;
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
                    if(doc == null)
                    {
                        context.SetStatusToNotFound();
                        return;
                    }
                    context.WriteData(doc.Data, doc.Metadata,doc.Etag);
                    break;
                case "DELETE":
                    var etag = context.GetEtag();
                    if(etag ==Guid.Empty)
                    {
                        context.SetStatusToBadRequest();
                        context.Write("Invalid ETag for DELETE opeartion");
                        return;
                    }
                    Database.Delete(docId, etag);
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
            var id = Database.Put(docId, context.GetEtag(), json, context.Request.Headers.FilterHeaders());
            context.WriteJson(new { id });
        }
    }
}