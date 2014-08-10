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
		private string RavenReplicationHilo;

		public ReplicationHiLo()
		{
			RavenReplicationHilo = "Raven/Replication/Hilo";
		}

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
			if (incrementedCurrent <= currentMax.Value)
				return incrementedCurrent;
			lock (generatorLock)
			{
				incrementedCurrent = Interlocked.Increment(ref current);
				if (incrementedCurrent <= currentMax.Value)
					return incrementedCurrent;
				if (current > currentMax.Value)
				{
					currentMax = new Hodler(GetNextMax());
				}
				return Interlocked.Increment(ref current);
			}
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
					using (Database.TransactionalStorage.DisableBatchNesting())
					{
						var minNextMax = currentMax.Value;
						long max = 0;
						Database.TransactionalStorage.Batch(accessor =>
						{
							var val = accessor.General.GetNextIdentityValue(RavenReplicationHilo, 0);
							var next = Math.Max(minNextMax, val);
							current = next + 1;
							max = next + capacity;
							accessor.General.SetIdentityValue(RavenReplicationHilo, max);
						});
						return max;
					}
				}
				catch (ConcurrencyException)
				{
				}
			}
		}
	}
}