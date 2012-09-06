using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using IronJS.Hosting;
using Raven.Abstractions;
using Raven.Abstractions.Data;

namespace Raven.Database.Json
{
	public class ScriptsCache
	{
		private class CachedResult
		{
			public int Usage;
			public DateTime Timestamp;
			public ConcurrentQueue<CSharp.Context> Queue;
		}

		private const int CacheMaxSize = 250;

		private readonly ConcurrentDictionary<ScriptedPatchRequest, CachedResult> cacheDic =
			new ConcurrentDictionary<ScriptedPatchRequest, CachedResult>();

		public void CheckinScript(ScriptedPatchRequest request, CSharp.Context context)
		{
			CachedResult value;
			if (cacheDic.TryGetValue(request, out value))
			{
				if (value.Queue.Count > 20)
					return;
				value.Queue.Enqueue(context);
				return;
			}
			var queue = new ConcurrentQueue<CSharp.Context>();
			queue.Enqueue(context);
			cacheDic.TryAdd(request, new CachedResult
			{
				Queue = queue,
				Timestamp = SystemTime.UtcNow,
				Usage = 1
			});
		}

		public CSharp.Context CheckoutScript(ScriptedPatchRequest request)
		{
			CachedResult value;
			if (cacheDic.TryGetValue(request, out value))
			{
				Interlocked.Increment(ref value.Usage);
				CSharp.Context context;
				if (value.Queue.TryDequeue(out context))
				{
					return context;
				}
			}
			var result = ScriptedJsonPatcher.CreateContext(request);

			var cachedResult = new CachedResult
			{
				Usage = 1,
				Queue = new ConcurrentQueue<CSharp.Context>(),
				Timestamp = SystemTime.UtcNow
			};

			cacheDic.AddOrUpdate(request, cachedResult, (_, existing) =>
			{
				Interlocked.Increment(ref existing.Usage);
				return existing;
			});
			if (cacheDic.Count > CacheMaxSize)
			{
				foreach (var source in cacheDic
					.OrderByDescending(x => x.Value.Usage)
					.ThenBy(x => x.Value.Timestamp)
					.Skip(CacheMaxSize))
				{
					if (Equals(source.Key, request))
						continue; // we don't want to remove the one we just added
					CachedResult ignored;
					cacheDic.TryRemove(source.Key, out ignored);
				}
			}

			return result;
		}
	}
}