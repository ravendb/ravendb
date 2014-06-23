using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Prefetching;

namespace Raven.Database.Server.Responders.Debugging
{
	public class DebugPrefetchStatus : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return @"^/debug/prefetch-status"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET" }; }
		}

		public override void Respond(Abstractions.IHttpContext context)
		{
			var prefetcherDocs = Database.IndexingExecuter.PrefetchingBehavior.DebugGetDocumentsInPrefetchingQueue().ToArray();
			var compareToCollection = new Dictionary<Etag, int>();

			for (int i = 1; i < prefetcherDocs.Length; i++)
				compareToCollection.Add(prefetcherDocs[i-1].Etag, prefetcherDocs[i].Etag.CompareTo(prefetcherDocs[i-1].Etag));

			if (compareToCollection.Any(x => x.Value < 0))
			{
				context.WriteJson(new
				{
					HasCorrectlyOrderedEtags = true,
					EtagsWithKeys = prefetcherDocs.ToDictionary(x => x.Etag, x => x.Key)
				});
			}
			else
			{
				context.WriteJson(new
				{
					HasCorrectlyOrderedEtags = false,
					IncorrectlyOrderedEtags = compareToCollection.Where(x => x.Value < 0),
					EtagsWithKeys = prefetcherDocs.ToDictionary(x => x.Etag, x => x.Key)
				});
			}
		}
	}
}
