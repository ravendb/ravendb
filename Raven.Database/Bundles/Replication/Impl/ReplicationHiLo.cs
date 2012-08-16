//-----------------------------------------------------------------------
// <copyright file="ReplicationHiLo.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Database;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Impl
{
	internal class ReplicationHiLo
	{
		private readonly object generatorLock = new object();
		private volatile Hodler currentMax = new Hodler(0);
		private long capacity = 256;
		private long current;
		private DateTime lastRequestedUtc;
		public DocumentDatabase Database { get; set; }

		private class Hodler
		{
			public readonly long Value;

			public Hodler(long value)
			{
				Value = value;
			}
		}

		public long NextId()
		{
			long incrementedCurrent = Interlocked.Increment(ref current);
			while (incrementedCurrent > currentMax.Value)
			{
				lock (generatorLock)
				{
					if (current > currentMax.Value)
					{
						currentMax = new Hodler(GetNextMax());
					}
					return Interlocked.Increment(ref current);
				}
			}
			return incrementedCurrent;

		}

		private long GetNextMax()
		{
			var span = SystemTime.UtcNow - lastRequestedUtc;
			if (span.TotalSeconds < 1)
			{
				capacity *= 2;
			}
			lastRequestedUtc = SystemTime.UtcNow;
			while (true)
			{
				try
				{
					var minNextMax = currentMax.Value;
					var document = Database.Get(Constants.RavenReplicationVersionHiLo, null);
					if (document == null)
					{
						Database.Put(Constants.RavenReplicationVersionHiLo,
									 Guid.Empty,
									 // sending empty guid means - ensure the that the document does NOT exists
									 RavenJObject.FromObject(RavenJObject.FromObject(new { Max = minNextMax + capacity })),
									 new RavenJObject(),
									 null);
						return minNextMax + capacity;
					}
					var max = GetMaxFromDocument(document, minNextMax);
					document.DataAsJson["Max"] = max + capacity;
					Database.Put(Constants.RavenReplicationVersionHiLo, document.Etag,
								 document.DataAsJson,
								 document.Metadata, null);
					current = max + 1;
					return max + capacity;
				}
				catch (ConcurrencyException)
				{
					// expected, we need to retry
				}
			}
		}

		private long GetMaxFromDocument(JsonDocument document, long minMax)
		{
			long max;
			if (document.DataAsJson.ContainsKey("ServerHi")) // convert from hi to max
			{
				var hi = document.DataAsJson.Value<long>("ServerHi");
				max = ((hi - 1) * capacity);
				document.DataAsJson.Remove("ServerHi");
				document.DataAsJson["Max"] = max;
			}
			max = document.DataAsJson.Value<long>("Max");
			return Math.Max(max, minMax);
		}

	}
}