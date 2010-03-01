using System;
using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Root : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET"}; }
        }

        protected override void Respond(KayakContext context)
        {
            context.Response.Redirect("/divan/index.html");
        }
    }
}