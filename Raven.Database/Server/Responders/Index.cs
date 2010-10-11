using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;
using System.Linq;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

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
		    	var indexDefinition = Database.GetIndexDefinition(index);
				if(indexDefinition == null)
				{
					context.SetStatusToNotFound();
					return;
				}
		    	context.WriteJson(new {Index = indexDefinition});
		    }
		    else
            {                
				var indexQuery = context.GetIndexQueryFromHttpContext(Database.Configuration.MaxPageSize);
                
                QueryResult queryResult = null;
                if (index.StartsWith("dynamic", StringComparison.InvariantCultureIgnoreCase))
                {
                    string entityName = null;
                    if (index.StartsWith("dynamic/"))
                        entityName = index.Substring("dynamic/".Length);
                    queryResult = Database.ExecuteDynamicQuery(entityName, indexQuery);
                }
                else
                {
                    queryResult = Database.Query(index, indexQuery);
                    Database.Query(index, indexQuery);
                }                
                
            	var includes = context.Request.QueryString.GetValues("include") ?? new string[0];
            	var loadedIds = new HashSet<string>(
            		queryResult.Results
            			.Where(x => x["@metadata"] != null)
            			.Select(x => x["@metadata"].Value<string>("@id"))
            			.Where(x => x != null)
            		);
            	var command = new AddIncludesCommand(Database, GetRequestTransaction(context), queryResult.Includes.Add,includes, loadedIds);
            	foreach (var result in queryResult.Results)
            	{
            		command.Execute(result);
            	}
            	context.WriteJson(queryResult);
			}
		}

	}
}