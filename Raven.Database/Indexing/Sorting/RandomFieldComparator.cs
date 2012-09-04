using System;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Database.Indexing.Sorting
{
	public class RandomFieldComparator : FieldComparator
	{
		private readonly Random random;
		private readonly int[] values;
		private int bottom; // Value of bottom of queue
		private int[] currentReaderValues;

		internal RandomFieldComparator(int numHits, String field)
		{
			values = new int[numHits];
			random = new Random(field.GetHashCode());
		}

		public override int Compare(int slot1, int slot2)
		{
			int v1 = values[slot1];
			int v2 = values[slot2];
			if (v1 > v2)
				return 1;
			if (v1 < v2)
				return -1;
			return 0;
		}

		public override int CompareBottom(int doc)
		{
			int v2 = currentReaderValues[doc];
			if (bottom > v2)
				return 1;
			if (bottom < v2)
				return -1;
			return 0;
		}

		public override void Copy(int slot, int doc)
		{
			values[slot] = currentReaderValues[doc];
		}

		public override void SetNextReader(IndexReader reader, int docBase)
		{
			currentReaderValues = new int[reader.MaxDoc];
			for (int i = 0; i < currentReaderValues.Length; i++)
			{
				currentReaderValues[i] = random.Next();
			}
		}

		public override IComparable this[int slot]
		{
			get { return values[slot]; }
		}

		public override void SetBottom(int bottom)
		{
			this.bottom = values[bottom];
		}
	}
}