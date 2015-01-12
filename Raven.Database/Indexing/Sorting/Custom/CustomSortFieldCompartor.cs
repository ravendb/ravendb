// -----------------------------------------------------------------------
//  <copyright file="CustomSortFieldCompartor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Database.Indexing.Sorting.Custom
{
	public class CustomSortFieldCompartor : FieldComparator
	{
		private readonly IndexEntriesToComparablesGenerator generator;
		private readonly IComparable[] values;
		private IComparable bottom;
		private IndexReader currentReader;

		public CustomSortFieldCompartor(IndexEntriesToComparablesGenerator generator, int numHits)
		{
			this.generator = generator;
			values= new IComparable[numHits];
		}

		public override int Compare(int slot1, int slot2)
		{
			var a = values[slot1];
			var b = values[slot2];
			return a.CompareTo(b);
		}

		public override void SetBottom(int slot)
		{
			bottom = values[slot];
		}

		public override int CompareBottom(int doc)
		{
			var current = generator.Generate(currentReader,doc);
			return bottom.CompareTo(current);
		}

		public override void Copy(int slot, int doc)
		{
			values[slot] = generator.Generate(currentReader, doc);
		}

		public override void SetNextReader(IndexReader reader, int docBase)
		{
			currentReader = reader;
		}

		public override IComparable this[int slot]
		{
			get { return values[slot]; }
		}
	}
}