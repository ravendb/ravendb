using System;
using System.Net;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;
using System.Linq;

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
			var data = context.ReadJsonObject<IndexDefinition>();
			if (data.Map == null)
			{
				context.SetStatusToBadRequest();
				context.Write("Expected json document with 'Map' property");
				return;
			}
			context.SetStatusToCreated("/indexes/" + index);
			context.WriteJson(new { Index = Database.PutIndex(index, data) });
		}

		private void OnGet(HttpListenerContext context, string index)
		{
			var definition = context.Request.QueryString["definition"];
			if ("yes".Equals(definition, StringComparison.InvariantCultureIgnoreCase))
			{
				context.WriteJson(new {Index = Database.IndexDefinitionStorage.GetIndexDefinition(index)});
			}
			else
			{
				context.WriteJson(Database.Query(index, new IndexQuery
				{
					Query = context.Request.QueryString["query"],
					Start = context.GetStart(),
					PageSize = context.GetPageSize(),
					FieldsToFetch = context.Request.QueryString.GetValues("fetch"),
					SortedFields = context.Request.QueryString.GetValues("sort")
						.EmptyIfNull()
						.Select(x => new SortedField(x))
						.ToArray()
				}));
			}
		}
	}
}