using System.Net;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Server.Responders
{
    public class Docs : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/docs/?$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET", "POST"}; }
        }

        public override void Respond(HttpListenerContext context)
        {
            switch (context.Request.HttpMethod)
            {
                case "GET":
                    context.WriteJson(Database.GetDocuments(context.GetStart(), context.GetPageSize()));
                    break;
                case "POST":
                    var json = context.ReadJson();
                    var id = Database.Put(null, json, new JObject());

                    context.SetStatusToCreated("/docs/" + id);
                    context.WriteJson(new { id });
                    break;
            }
        }
    }
}