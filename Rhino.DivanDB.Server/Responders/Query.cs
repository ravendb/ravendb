using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Query : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/queries"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET"}; }
        }

        protected override void Respond(KayakContext context)
        {
            context.WriteJson(Database.ViewStorage.ViewNames);
        }
    }
}