using Kayak;

namespace Rhino.DivanDB.Server.Responders
{
    public class Indexes : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/indexes/?$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET"}; }
        }

        protected override void Respond(KayakContext context)
        {
            context.WriteJson(Database.IndexDefinitionStorage.IndexNames);
        }
    }
}