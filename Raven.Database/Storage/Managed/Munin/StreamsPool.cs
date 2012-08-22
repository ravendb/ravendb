//-----------------------------------------------------------------------
// <copyright file="StreamsPool.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Munin
{
	public class StreamsPool : IDisposable
	{
		private readonly Func<Stream> createNewStream;
		private readonly ConcurrentDictionary<int, ConcurrentQueue<Stream>> openedStreamsPool = new ConcurrentDictionary<int, ConcurrentQueue<Stream>>();
		private int version;

		public StreamsPool(Func<Stream> createNewStream)
		{
			this.createNewStream = createNewStream;
			openedStreamsPool.TryAdd(0, new ConcurrentQueue<Stream>());
		}

		public int Count
		{
			get
			{
				return openedStreamsPool.Sum(x => x.Value.Count);
			}
		}

		public void Clear()
		{
			Stream result;
			var currentVersion = Interlocked.Increment(ref version);
			openedStreamsPool.TryAdd(currentVersion, new ConcurrentQueue<Stream>());
			var keysToRemove = openedStreamsPool.Keys.Where(x => x < currentVersion).ToArray();

			foreach (var keyToRemove in keysToRemove)
			{
				ConcurrentQueue<Stream> value;
				if (openedStreamsPool.TryRemove(keyToRemove, out value) == false)
					continue;

				while (value.TryDequeue(out result))
				{
					try
					{
						result.Dispose();
					}
					catch { }
				}
			}
		}

		public void Dispose()
		{
			Stream result;
			var currentVersion = Interlocked.Increment(ref version);
			openedStreamsPool.TryAdd(currentVersion, new ConcurrentQueue<Stream>());
			var keysToRemove = openedStreamsPool.Keys.ToArray();

			foreach (var keyToRemove in keysToRemove)
			{
				ConcurrentQueue<Stream> value;
				if (openedStreamsPool.TryRemove(keyToRemove, out value) == false)
					continue;

				while (value.TryDequeue(out result))
				{
					result.Dispose();
				}
			}
		}

		public IDisposable Use(out Stream stream)
		{
			var currentversion = Thread.VolatileRead(ref version);
			ConcurrentQueue<Stream> current;
			openedStreamsPool.TryGetValue(currentversion, out current);
			Stream value = current != null && current.TryDequeue(out value) ? 
				value : 
				createNewStream();
			stream = value;
			return new DisposableAction(delegate
			{
				ConcurrentQueue<Stream> current2;
				if (currentversion == Thread.VolatileRead(ref currentversion) && 
					openedStreamsPool.TryGetValue(currentversion, out current2))
				{
					current2.Enqueue(value);
				}
				else
				{
					value.Dispose();
				}
			});
		}
	}
}