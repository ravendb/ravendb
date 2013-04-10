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
			get { return new[] { "GET", "PUT", "DELETE" }; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var transformer = match.Groups[1].Value;
			switch (context.Request.HttpMethod)
			{
				case "GET":
					if (string.IsNullOrEmpty(transformer) == false && transformer != "/")
					{
						context.WriteJson(RavenJObject.FromObject(Database.GetTransformerDefinition(transformer)));
						break;
					}

					var namesOnlyString = context.Request.QueryString["namesOnly"];
					bool namesOnly;
					RavenJArray transformers;
					if (bool.TryParse(namesOnlyString, out namesOnly) && namesOnly)
						transformers = Database.GetTransformerNames(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
					else
						transformers = Database.GetTransformers(context.GetStart(), context.GetPageSize(Database.Configuration.MaxPageSize));
					context.WriteJson(transformers);
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
					context.WriteJson(new { Transformer = Database.PutTransform(transformer, data) });
					break;
				case "DELETE":
					context.SetStatusToDeleted();
					Database.DeleteTransfom(transformer);
					break;
			}
		}
	}
}