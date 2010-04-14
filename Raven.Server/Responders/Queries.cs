using System.Net;
using Newtonsoft.Json.Linq;

namespace Raven.Server.Responders
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

		public override void Respond(HttpListenerContext context)
		{
			var itemsToLoad = context.ReadJsonArray();
			var results = new JArray();
			Database.TransactionalStorage.Batch(actions =>
			{
				foreach (JToken item in itemsToLoad)
				{
					var documentByKey = actions.DocumentByKey(item.Value<string>());
					if (documentByKey == null)
						continue;
					results.Add(documentByKey.ToJson());
					actions.Commit();
				}
			});
			context.WriteJson(results);
		}
	}
}