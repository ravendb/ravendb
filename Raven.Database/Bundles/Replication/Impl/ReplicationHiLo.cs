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
		private volatile ValueHolder valueHolder = new ValueHolder(0);
		private long capacity = 256;
		private DateTime lastRequestedUtc;
		public DocumentDatabase Database { get; set; }

        public class ValueHolder
        {
            public readonly long MaxValue;

            public long Current;

            public ValueHolder(long maxValue)
            {
                MaxValue = maxValue;
            }
        }

        public long NextId()
        {
            var current = valueHolder;
            var incrementedCurrent = Interlocked.Increment(ref current.Current);

            while (incrementedCurrent > current.MaxValue)
            {
                lock (generatorLock)
                {
                    current = valueHolder;
                    if (Interlocked.Read(ref current.Current) > current.MaxValue)
                    {
                        using (var locker = Database.DocumentLock.TryLock(250))
                        {
                            if (locker == null)
                                continue;

                            current = valueHolder = GetNextMax(current.MaxValue);
                        }
                    }

                    incrementedCurrent = Interlocked.Increment(ref current.Current);
                }
            }

            return incrementedCurrent;
        }

        private ValueHolder GetNextMax(long currentMaxValue)
		{
			var span = SystemTime.UtcNow - lastRequestedUtc;
			if (span.TotalSeconds < 1)
			{
				capacity *= 2;
			}

			lastRequestedUtc = SystemTime.UtcNow;

			while (true)
			{
				IDisposable newBatchToRecoverFromConcurrencyException = null;

				try
				{
					using (newBatchToRecoverFromConcurrencyException)
					{
						var document = Database.Get(Constants.RavenReplicationVersionHiLo, null);
						if (document == null)
                        {
                            var newMaxValue = currentMaxValue + capacity;

                            Database.Put(Constants.RavenReplicationVersionHiLo,
								Etag.Empty,
								// sending empty etag means - ensure the that the document does NOT exists
								RavenJObject.FromObject(RavenJObject.FromObject(new {Max = newMaxValue })),
								new RavenJObject(),
								null);

							return new ValueHolder(newMaxValue);
						}
						var max = GetMaxFromDocument(document, currentMaxValue);
						document.DataAsJson["Max"] = max + capacity;
						Database.Put(Constants.RavenReplicationVersionHiLo, document.Etag,
							document.DataAsJson,
							document.Metadata, null);

						return new ValueHolder(max + capacity)
                        {
                            Current = max
                        };
					}
				}
				catch (ConcurrencyException)
				{
					// expected, we need to retry
					// but in a new transaction to avoid getting stuck in infinite loop because of concurrency exception

					newBatchToRecoverFromConcurrencyException = Database.TransactionalStorage.DisableBatchNesting();
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