using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Queries : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/queries/?$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET"}; }
        }

        protected override void Respond(KayakContext context)
        {
            switch (context.Request.Verb)
            {
                case "GET":
                    context.WriteJson(Database.IndexDefinitionStorage.IndexNames);
                    break;
            }
        }
    }
}