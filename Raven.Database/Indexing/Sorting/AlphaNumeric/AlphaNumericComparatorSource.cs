using System;
using System.Linq;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Database.Indexing.Sorting.AlphaNumeric
{
	public class AlphaNumericComparatorSource : FieldComparatorSource
	{
		public override FieldComparator NewComparator(string fieldname, int numHits, int sortPos, bool reversed)
		{
			return new AlphaNumericFieldComparator(numHits, fieldname);
		}

		private class AlphaNumericFieldComparator : FieldComparator
		{
			private readonly string[] values;
			private string[] currentReaderValues;
			private readonly string field;
			private string bottom;

			public AlphaNumericFieldComparator(int numHits, string field)
			{
				values = new string[numHits];
				this.field = field;
			}

			public override int Compare(int slot1, int slot2)
			{
				var str1 = values[slot1];
				var str2 = values[slot2];

				if (str1 == null)
					return str2 == null ? 0 : -1;
				else if (str2 == null)
					return 1;

				return AlphanumComparatorFast.Compare(str1, str2);
			}

			public override void SetBottom(int slot)
			{
				bottom = values[slot];
			}

			public override int CompareBottom(int doc)
			{
				var str2 = currentReaderValues[doc];
				if (bottom == null)
					return str2 == null ? 0 : -1;
				else if (str2 == null)
					return 1;

				return AlphanumComparatorFast.Compare(bottom, str2);
			}

			public override void Copy(int slot, int doc)
			{
				values[slot] = currentReaderValues[doc];
			}

			public override void SetNextReader(IndexReader reader, int docBase)
			{
				currentReaderValues = FieldCache_Fields.DEFAULT.GetStrings(reader, field);
			}

			public override IComparable this[int slot]
			{
				get { return values[slot]; }
			}
		}
	}
}
