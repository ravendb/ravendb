namespace Raven.Database.Server.Responders
{
	using Http.Abstractions;
	using Http.Extensions;

	public class Databases : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/databases/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			switch (context.Request.HttpMethod)
			{
				case "GET":
					context.WriteJson(Database.GetDocumentsWithIdStartingWith("Raven/Databases/"));
					break;
			}
		}
	}
}