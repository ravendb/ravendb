using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json.Linq;

namespace Raven.Storage.Managed.Data
{
	public class Queue 
	{
		private readonly Stream reader;
		private readonly Stream writer;
		private readonly Tree tree;
		private readonly BinaryReaderWith7BitEncoding binaryReader;
		private readonly BinaryWriterWith7BitEncoding binaryWriter;

		private long currentId;

		public long RootPosition { get; private set; }

		public Queue(Stream reader, Stream writer, StartMode mode)
		{
			this.reader = reader;
			this.writer = writer;

			binaryReader = new BinaryReaderWith7BitEncoding(reader);
			binaryWriter = new BinaryWriterWith7BitEncoding(writer);

			if(mode == StartMode.Open)
			{
				currentId = binaryReader.Read7BitEncodedInt64();
				var treePos = binaryReader.Read7BitEncodedInt64();
				reader.Position = treePos;
			}
			tree = new Tree(reader, writer, mode);
		}

		public void Flush()
		{
			tree.Flush();
			RootPosition = writer.Position;
			binaryWriter.Write7BitEncodedInt64(currentId);
			binaryWriter.Write7BitEncodedInt64(tree.RootPosition);
		}

		public long Enqueue(long value)
		{
			currentId += 1;
			tree.Add(JObject.FromObject(new { Id = currentId, Reads = 0}), value);
			return currentId;
		}

		public void Remove(long id)
		{
			tree.Remove(new JObject(new JProperty("Id", new JValue(currentId))));
		}

		public IEnumerable<Tuple<long, long>> Scan()
		{
			foreach (var treeNode in tree.IndexScan())
			{
				if (treeNode.NodeValue == null)
					continue;

				tree.Remove(treeNode.NodeKey);

				var readCount = treeNode.NodeKey.Value<int>("Reads") + 1;
				if(readCount > 5)
					continue;

				tree.Add(JObject.FromObject(new {Id = currentId, Reads = readCount}), 
					treeNode.NodeValue.Value);

				yield return new Tuple<long, long>(
					treeNode.NodeValue.Value,
					treeNode.NodeKey.Value<long>("Id")
					);
			}
		}
	}
}