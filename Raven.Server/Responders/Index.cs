using System;
using System.Net;
using Newtonsoft.Json.Linq;
using Raven.Database.Indexing;

namespace Raven.Server.Responders
{
	public class Index : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"/indexes/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET", "PUT", "DELETE"}; }
		}

		public override void Respond(HttpListenerContext context)
		{
			var match = urlMatcher.Match(context.Request.Url.LocalPath);
			var index = match.Groups[1].Value;

			switch (context.Request.HttpMethod)
			{
				case "GET":
					OnGet(context, index);
					break;
				case "PUT":
					Put(context, index);
					break;
				case "DELETE":
					context.SetStatusToDeleted();
					Database.DeleteIndex(index);
					break;
			}
		}

		private void Put(HttpListenerContext context, string index)
		{
			var data = context.ReadJson();
			var mapProp = data.Property("Map");
			if (mapProp == null)
			{
				context.SetStatusToBadRequest();
				context.Write("Expected json document with 'Map' property");
				return;
			}
			var mapDef = mapProp.Value.Value<string>();
			string reduceDef = null;
			if (data.Property("Reduce") != null)
				reduceDef = data.Property("Reduce").Value.Value<string>();

			context.SetStatusToCreated("/indexes/" + index);
			context.WriteJson(new
			{
				index = Database.PutIndex(index,
				                          mapDef,
				                          reduceDef
			                  	)
			});
		}

		private void OnGet(HttpListenerContext context, string index)
		{
			var definition = context.Request.QueryString["definition"];
			if ("yes".Equals(definition, StringComparison.InvariantCultureIgnoreCase))
			{
				context.WriteJson(new {index = Database.IndexDefinitionStorage.GetIndexDefinition(index)});
			}
			else
			{
				context.WriteJson(Database.Query(index, new IndexQuery(
				                                        	context.Request.QueryString["query"], 
															context.GetStart(),
				                                        	context.GetPageSize(),
				                                        	context.Request.QueryString.GetValues("fetch"))));
			}
		}
	}
}