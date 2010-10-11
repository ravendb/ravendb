using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Queries : RequestResponder
	{
		public override string UrlPattern
		{
			get { return "/queries/?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"POST","GET"}; }
		}

		public override void Respond(IHttpContext context)
		{
			JArray itemsToLoad;
			if(context.Request.HttpMethod == "POST")
				itemsToLoad = context.ReadJsonArray();
			else
				itemsToLoad = new JArray(context.Request.QueryString.GetValues("id"));
			var result = new MultiLoadResult();
			var loadedIds = new HashSet<string>();
			var includes = context.Request.QueryString.GetValues("include") ?? new string[0];
			var transactionInformation = GetRequestTransaction(context);
			Database.TransactionalStorage.Batch(actions =>
			{
				var addIncludesCommand = new AddIncludesCommand(Database, transactionInformation, result.Includes.Add, includes, loadedIds);
				foreach (JToken item in itemsToLoad)
				{
					var value = item.Value<string>();
					if(loadedIds.Add(value)==false)
						continue;
					var documentByKey = actions.Documents.DocumentByKey(value,
                        transactionInformation);
					if (documentByKey == null)
						continue;
					result.Results.Add(documentByKey.ToJson());

					addIncludesCommand.Execute(documentByKey.DataAsJson);
				}
            });
			context.WriteJson(result);
		}
	}
}
