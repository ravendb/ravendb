using Raven.Abstractions.Indexing;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Database.Server.Responders
{
	public class Transformers : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/transformers(/.+)?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET", "PUT" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[1].Value;
			switch (context.Request.HttpMethod)
			{
				case "GET":
					if (string.IsNullOrEmpty(index) == false && index != "/")
					{
						context.SetStatusToBadRequest();
						context.WriteJson(new
						{
							Error = "Cannot GET from a specific transformer but got a request for: " + index.Substring(0)
						});
						return;
					}

					var namesOnlyString = context.Request.QueryString["namesOnly"];
					bool namesOnly;
					RavenJArray indexes;
					if (bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
						indexes = Database.GetTransformerNames(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
					else
						indexes = Database.GetTransformers(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
					context.WriteJson(indexes);
					break;
				case "PUT":
					var data = context.ReadJsonObject<TransformerDefinition>();
					if (data == null || string.IsNullOrEmpty(data.TransformResults))
					{
						context.SetStatusToBadRequest();
						context.Write("Expected json document with 'TransformResults' property");
						return;
					}
					context.SetStatusToCreated("/transformers");
					context.WriteJson(new { Transfomer = Database.PutTransform(index, data) });
					break;
			}
		}
	}
}