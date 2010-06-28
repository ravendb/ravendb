using System.Collections.Generic;
using System.IO;

namespace Raven.Storage.Managed.Data
{
	public class TreeOfQueues
	{
		private readonly Stream reader;
		private readonly Stream writer;
		private readonly Tree queues;
		private readonly IDictionary<string, Queue> queuesInMem = new Dictionary<string, Queue>();

		public TreeOfQueues(Stream reader, Stream writer, StartMode mode)
		{
			this.reader = reader;
			this.writer = writer;
			queues = new Tree(reader, writer, mode);
		}

		public long RootPosition
		{
			get
			{
				return queues.RootPosition;
			}
		}

		public Queue GetQueue(string name)
		{
			Queue value;
			if (queuesInMem.TryGetValue(name, out value))
				return value;
			var queuePos = queues.FindValue(name);
			if (queuePos == null)
				return null;
			reader.Position = queuePos.Value;
			var q = new Queue(reader, writer, StartMode.Open);
			queuesInMem.Add(name, q);
			return q;
		}

		public Queue GetOrCreateQueue(string name)
		{
			var queue = GetQueue(name);
			if (queue != null)
				return queue;
			var pos = writer.Position;
			queue = new Queue(reader, writer, StartMode.Create);
			queue.Flush();
			queuesInMem.Add(name, queue);
			queues.Add(name, pos);
			return queue;
		}

		public void Flush()
		{
			foreach (var queue in queuesInMem)
			{
				queue.Value.Flush();
				queues.Add(queue.Key, queue.Value.RootPosition);
			}
			queues.Flush();
		}
	}
}