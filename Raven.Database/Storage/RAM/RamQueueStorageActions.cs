using System;
using System.Collections.Generic;
using Raven.Database.Impl;
using System.Linq;

namespace Raven.Database.Storage.RAM
{
	public class RamQueueStorageActions : IQueueStorageActions
	{
		private readonly RamState state;
		private readonly IUuidGenerator generator;

		public RamQueueStorageActions(RamState state, IUuidGenerator generator)
		{
			this.state = state;
			this.generator = generator;
		}

		public void EnqueueToQueue(string name, byte[] data)
		{
			state.Queues.GetOrAdd(name).Set(generator.CreateSequentialUuid(), data);
		}

		public IEnumerable<Tuple<byte[], object>> PeekFromQueue(string name)
		{
			return state.Queues.GetOrAdd(name)
				.OrderBy(x => x.Key)
				.Select(pair => Tuple.Create(pair.Value, (object) pair.Key));
		}

		public void DeleteFromQueue(string name, object id)
		{
			state.Queues.GetOrAdd(name).Remove((Guid) id);
		}
	}
}