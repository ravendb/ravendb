using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace Raven.Storage.Managed.Data
{
	public class Bag : IEnumerable<long>
	{
		private readonly Stream reader;
		private readonly Stream writer;
		private ListItem current;
		private readonly BinaryWriterWith7BitEncoding binaryWriter;
		private readonly BinaryReaderWith7BitEncoding binaryReader;
		private readonly List<long> unwritten = new List<long>();

		private class ListItem
		{
			public long Position { get; set; }
			public readonly ListItem NextItem;
			public readonly long[] Values;
			public readonly long? Next;

			public ListItem(long[] values, ListItem nextItem)
			{
				Values = values;
				NextItem = nextItem;
			}

			public ListItem(long[] values,  long? next)
			{
				Values = values;
				Next = next;
			}
		}

		public long? CurrentPosition
		{
			get
			{
				return current == null ? (long?)null : current.Position;
			}
		}


		public Bag(Stream reader, Stream writer, StartMode mode)
		{
			this.reader = reader;
			this.writer = writer;

			binaryReader = new BinaryReaderWith7BitEncoding(reader);
			binaryWriter = new BinaryWriterWith7BitEncoding(writer);

			if (mode != StartMode.Open)
				return;
			current = ReadItem(reader.Position);
		}

		public void Add(long data)
		{
			unwritten.Add(data);
		}

		private ListItem GetNextItem(ListItem item)
		{
			if (item == null)
				return null;
			
			if (item.NextItem != null)
				return item.NextItem;
			if (item.Next != null)
				return ReadItem(item.Next.Value);
			return null;
		}

		public void Flush()
		{
			if (unwritten.Count == 0)
				return;

			var pos = writer.Position;

			binaryWriter.Write7BitEncodedInt(unwritten.Count);
			foreach (var value in unwritten)
			{
				binaryWriter.Write7BitEncodedInt64(value);
			}
			binaryWriter.WriteBitEncodedNullableInt64(current == null ? (long?)null : current.Position);
			current = new ListItem(unwritten.ToArray(), current)
			{
				Position = pos
			};
			unwritten.Clear();
		}

		private ListItem ReadItem(long position)
		{
			reader.Position = position;
			var len = binaryReader.Read7BitEncodedInt();
			var values = new long[len];
			for (int i = 0; i < len; i++)
			{
				values[i] = binaryReader.Read7BitEncodedInt64();
			}
			return new ListItem(
				values, 
				binaryReader.ReadBitEncodedNullableInt64()
				)
			{
				Position = position
			};
		}

		public IEnumerator<long> GetEnumerator()
		{
			var item = current;
			while (item != null)
			{
				foreach (var t in item.Values)
				{
					yield return t;
				}
				item = GetNextItem(item);
			}
			foreach (var l in unwritten)
			{
				yield return l;
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void Remove(long value)
		{
			var item = current;
			var items = new List<ListItem>();
			ListItem lastUnmodifiedItem = null;
			while (item != null)
			{
				items.Add(item);
				if (Array.IndexOf(item.Values, value) == -1)
				{
					lastUnmodifiedItem = item;
				}
				else
				{
					unwritten.AddRange(item.Values);
				}
				item = GetNextItem(item);
			}
			current = lastUnmodifiedItem;
			unwritten.Remove(value);

		}
	}
}