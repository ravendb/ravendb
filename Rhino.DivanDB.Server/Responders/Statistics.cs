using System;
using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Statistics : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/stats"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[]{"GET"}; }
        }

        protected override void Respond(KayakContext context)
        {
            context.WriteJson(new { docCount = Database.CountOfDocuments });
        }
    }
}