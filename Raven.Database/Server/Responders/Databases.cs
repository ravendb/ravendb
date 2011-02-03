namespace Raven.Database.Server.Responders
{
	using Http.Abstractions;
	using Http.Extensions;

	/// <summary>
	/// HACK: Christopher's short term solution to getting the list of tenant databases
	/// </summary>
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
					context.WriteJson(Database.GetTenantDocuments());
					break;
			}
		}
	}
}