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
            get { return new[]{"GET","PUT"}; }
        }

        protected override void Respond(KayakContext context)
        {
            switch (context.Request.Verb)
            {
                case "PUT":
                    context.Response.SetStatusToCreated();
                    context.WriteJson(new { id = Database.Put(context.ReadJson()) });
                    break;
                case "GET":
                    context.WriteJson(new { docCount = Database.CountOfDocuments });
                    break;
            }
        }
    }
}