using Newtonsoft.Json.Linq;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Indexes : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/indexes/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var namesOnlyString = context.Request.QueryString["namesOnly"];
			bool namesOnly;
			JArray indexes;
			if(bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
				indexes = Database.GetIndexNames(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
			else
				indexes = Database.GetIndexes(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
			context.WriteJson(indexes);
		}
	}
}
