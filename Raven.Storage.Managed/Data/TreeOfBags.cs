using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Raven.Storage.Managed.Data
{
	public class TreeOfBags
	{
		private readonly Tree bags;
		private readonly Stream reader;
		private readonly Stream writer;

		private readonly IDictionary<JToken, Bag> bagsInMem = new Dictionary<JToken, Bag>(new JTokenEqualityComparer());

		public TreeOfBags(Stream reader, Stream writer, StartMode mode)
		{
			this.reader = reader;
			this.writer = writer;
			bags = new Tree(reader, writer, mode);
		}

		public long RootPosition
		{
			get
			{
				return bags.RootPosition;
			}
		}

		public Bag GetBag(JToken key)
		{
			Bag value;
			if (bagsInMem.TryGetValue(key, out value))
				return value;

			var pos = bags.FindValue(key);
			if (pos == null)
				return null;
			reader.Position = pos.Value;
			value = new Bag(reader,writer, StartMode.Open);
			bagsInMem.Add(key, value);
			return value;
		}

		public Bag GetOrCreateBag(JToken key)
		{
			var value = GetBag(key);
			if (value != null)
				return value;

			value = new Bag(reader, writer, StartMode.Create);
			bagsInMem.Add(key, value);
			return value;
		}

		public void Flush()
		{
			foreach (var queue in bagsInMem)
			{
				queue.Value.Flush();
				if (queue.Value.CurrentPosition == null)
					bags.Remove(queue.Key);
				else
					bags.Add(queue.Key, queue.Value.CurrentPosition.Value);
			}
			bags.Flush();
		}

		public void DeleteAllMatching(JToken key)
		{
			var comparer = new JTokenComparer();
			foreach (var toRemove in bagsInMem
				.Where(x => comparer.Compare(x.Key, key)==0)
				.ToArray())
			{
				bagsInMem.Remove(toRemove);
			}

			foreach (var treeNode in bags.ScanFromInclusive(key)
				.TakeWhile(x => comparer.Compare(key, x.NodeKey) == 0))
			{
				bags.Remove(treeNode.NodeKey);
			}
		}
	}
}