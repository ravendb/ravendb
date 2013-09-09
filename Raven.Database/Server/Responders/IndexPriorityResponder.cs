using System;
using Raven.Abstractions.Data;
using Raven.Database.Server.Abstractions;
using Raven.Database.Extensions;

namespace Raven.Database.Server.Responders
{
	public class IndexPriorityResponder : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/indexes/set-priority/(.+)"; }
		}
		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}
		public override void Respond(IHttpContext context)
		{

			var match = urlMatcher.Match(context.GetRequestUrl());
			var index = match.Groups[1].Value;

			IndexingPriority indexingPriority;
			if (Enum.TryParse(context.Request.QueryString["priority"], out indexingPriority) == false)
			{
				context.SetStatusToBadRequest();
				context.WriteJson(new
				{
					Error = "Could not parse priority value: " + context.Request.QueryString["priority"]
				});
				return;
			}

		    var instance = Database.IndexStorage.GetIndexInstance(index);
			Database.TransactionalStorage.Batch(accessor => accessor.Indexing.SetIndexPriority(instance.indexId, indexingPriority));
		}
	}
}
