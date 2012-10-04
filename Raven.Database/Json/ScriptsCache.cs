using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;

namespace Raven.Database.Json
{
	[CLSCompliant(false)]
	public class ScriptsCache
	{
		private class CachedResult
		{
			public int Usage;
			public DateTime Timestamp;
			public ConcurrentQueue<Jint.JintEngine> Queue;
		}

		private const int CacheMaxSize = 250;

		private readonly ConcurrentDictionary<ScriptedPatchRequest, CachedResult> cacheDic =
			new ConcurrentDictionary<ScriptedPatchRequest, CachedResult>();

		public void CheckinScript(ScriptedPatchRequest request, Jint.JintEngine context)
		{
			CachedResult value;
			if (cacheDic.TryGetValue(request, out value))
			{
				if (value.Queue.Count > 20)
					return;
				value.Queue.Enqueue(context);
				return;
			}
			cacheDic.AddOrUpdate(request, patchRequest =>
			{
				var queue = new ConcurrentQueue<Jint.JintEngine>();
				queue.Enqueue(context);
				return new CachedResult
				{
					Queue = queue,
					Timestamp = SystemTime.UtcNow,
					Usage = 1
				};
			}, (patchRequest, result) =>
			{
				result.Queue.Enqueue(context);
				return result;
			});
		}

		public Jint.JintEngine CheckoutScript(ScriptedPatchRequest request)
		{
			CachedResult value;
			if (cacheDic.TryGetValue(request, out value))
			{
				Interlocked.Increment(ref value.Usage);
				Jint.JintEngine context;
				if (value.Queue.TryDequeue(out context))
				{
					return context;
				}
			}
			var result = ScriptedJsonPatcher.CreateEngine(request);

			var cachedResult = new CachedResult
			{
				Usage = 1,
				Queue = new ConcurrentQueue<Jint.JintEngine>(),
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