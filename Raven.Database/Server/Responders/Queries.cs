using Newtonsoft.Json.Linq;
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
			var results = new JArray();
			Database.TransactionalStorage.Batch(actions =>
			{
				foreach (JToken item in itemsToLoad)
				{
					var documentByKey = actions.Documents.DocumentByKey(item.Value<string>(),
                        GetRequestTransaction(context));
					if (documentByKey == null)
						continue;
					results.Add(documentByKey.ToJson());
				}
            });
			context.WriteJson(results);
		}
	}
}