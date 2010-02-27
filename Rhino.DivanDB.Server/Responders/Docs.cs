using System;
using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Docs : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/docs/?$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET","POST"}; }
        }

        protected override void Respond(KayakContext context)
        {
            switch (context.Request.Verb)
            {
                case "POST":
                    context.Response.SetStatusToCreated();
                    var json = context.ReadJson();
                    var idProp = json.Property("_id");
                    if (idProp != null) 
                    {
                        context.Response.SetStatusToBadRequest();
                        context.Response.WriteLine("POST to " + context.Request.RequestUri +" with a document conatining '_id'");
                        return;
                    }
                    context.WriteJson(new { id = Database.Put(json) });
                    break;
                case "GET":
                    context.WriteJson(new { docCount = Database.CountOfDocuments });
                    break;
            }
        }
    }
}