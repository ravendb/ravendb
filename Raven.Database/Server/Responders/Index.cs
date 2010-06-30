using System;
using Raven.Database.Data;
using Raven.Database.Indexing;
using System.Linq;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Index : RequestResponder
	{
		public override string UrlPattern
		{
			get { return @"/indexes/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET", "PUT", "DELETE","HEAD","RESET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[1].Value;

			switch (context.Request.HttpMethod)
			{
                case "HEAD":
			        if(Database.IndexDefinitionStorage.IndexNames.Contains(index, StringComparer.InvariantCultureIgnoreCase) == false)
                        context.SetStatusToNotFound();
			        break;
				case "GET":
					OnGet(context, index);
					break;
				case "PUT":
					Put(context, index);
					break;
				case "RESET":
					if (BuiltinIndex(index, context))
						return;
					Database.ResetIndex(index);
					context.WriteJson(new {Reset = index});
					break;
				case "DELETE":
					if(index.StartsWith("Raven/",StringComparison.InvariantCultureIgnoreCase))
					{
						context.SetStatusToForbidden();
						context.WriteJson(new
						{
							Url = context.Request.RawUrl,
							Error = "Builtin indexes cannot be deleted, attempt to delete index '" + index + "' was rejected"
						});
						return;
					}
					context.SetStatusToDeleted();
					Database.DeleteIndex(index);
					break;
			}
		}

		private void Put(IHttpContext context, string index)
		{
			if (BuiltinIndex(index, context))
				return;
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

		private static bool BuiltinIndex(string index, IHttpContext context)
		{
			if (!index.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))
				return false;

			context.SetStatusToForbidden();
			context.WriteJson(new
			{
				Url = context.Request.RawUrl,
				Error = "Builtin indexes cannot be modified, attempt to modifiy index '" + index + "' was rejected"
			});
			return true;
		}

		private void OnGet(IHttpContext context, string index)
		{
			var definition = context.Request.QueryString["definition"];
		    if ("yes".Equals(definition, StringComparison.InvariantCultureIgnoreCase))
			{
				context.WriteJson(new {Index = Database.IndexDefinitionStorage.GetIndexDefinition(index)});
			}
			else
            {
                IndexQuery indexQuery = GetIndexQueryFromHttpContext(context);
                indexQuery.PageSize = Database.Configuration.MaxPageSize;
                context.WriteJson(Database.Query(index, indexQuery));
			}
		}

	    static public IndexQuery GetIndexQueryFromHttpContext(IHttpContext context)
	    {
	        return new IndexQuery
	        {
	            Query = Uri.UnescapeDataString(context.Request.QueryString["query"]),
	            Start = context.GetStart(),
	            Cutoff = context.GetCutOff(),
	            FieldsToFetch = context.Request.QueryString.GetValues("fetch"),
	            SortedFields = context.Request.QueryString.GetValues("sort")
	                .EmptyIfNull()
	                .Select(x => new SortedField(x))
	                .ToArray()
	        };
	    }
	}
}