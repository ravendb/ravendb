using System.Net;

namespace Rhino.DivanDB.Server.Responders
{
    public class Indexes : RequestResponder
    {
        public override string UrlPattern
        {
            get { return "/indexes/?$"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] {"GET"}; }
        }

        public override void Respond(HttpListenerContext context)
        {
<<<<<<< HEAD
            context.WriteJson(Database.IndexDefinitionStorage.IndexNames);
=======
            context.WriteJson(Database.GetIndexes(context.GetStart(), context.GetPageSize()));
>>>>>>> luke
        }
    }
}