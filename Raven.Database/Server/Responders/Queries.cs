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
			get { return new[] {"POST"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var itemsToLoad = context.ReadJsonArray();
			var results = new JArray();
			Database.TransactionalStorage.Batch(actions =>
			{
				foreach (JToken item in itemsToLoad)
				{
					var documentByKey = actions.DocumentByKey(item.Value<string>(),
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