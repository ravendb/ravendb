using System;
using System.Collections.Generic;
using Raven.Database.Storage.StorageActions;
using System.Linq;

namespace Raven.Storage.Managed.StroageActions
{
	public class QueueStorageActions : AbstractStorageActions, IQueueStorageActions
	{
		public void EnqueueToQueue(string name, byte[] data)
		{
			var pos = Writer.Position;
			BinaryWriter.Write7BitEncodedInt(data.Length);
			Writer.Write(data, 0, data.Length);
			Mutator.Queues
				.GetOrCreateQueue(name)
				.Enqueue(pos);
		}

		public IEnumerable<Tuple<byte[], long>> PeekFromQueue(string name)
		{
			var queue = Mutator.Queues.GetQueue(name);
			if (queue == null)
			{
				return new Tuple<byte[], long>[0];
			}
			return queue.Scan()
				.Select(tuple =>
				{
					Reader.Position = tuple.Item1;
					var size = BinaryReader.Read7BitEncodedInt();
					return new Tuple<byte[], long>(
						BinaryReader.ReadBytes(size),
						tuple.Item2
						);
				});
		}

		public void DeleteFromQueue(string name, long id)
		{
			var queue = Mutator.Queues.GetQueue(name);
			if (queue == null)
			{
				return;
			}
			queue.Remove(id);
		}
	}
}