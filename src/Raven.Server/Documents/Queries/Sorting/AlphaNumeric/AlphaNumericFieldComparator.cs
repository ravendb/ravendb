using System;
using System.Runtime.InteropServices;

using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class AlphaNumericFieldComparator : FieldComparator
    {
        private readonly string[] values;
        private readonly string field;
        private string bottom;
        private int[] order;
        private string[] lookup;

        public AlphaNumericFieldComparator(int numHits, string field)
        {
            values = new string[numHits];
            this.field = field;
        }

        private static class SafeNativeMethods
        {
            [DllImport("shlwapi.dll", CharSet = CharSet.Unicode)]
            public static extern int StrCmpLogicalW(string psz1, string psz2);
        }

        public override int Compare(int slot1, int slot2)
        {
            var str1 = values[slot1];
            var str2 = values[slot2];

            if (str1 == null)
                return str2 == null ? 0 : -1;
            if (str2 == null)
                return 1;

            return SafeNativeMethods.StrCmpLogicalW(str1, str2);
        }

        public override void SetBottom(int slot)
        {
            bottom = values[slot];
        }

        public override int CompareBottom(int doc)
        {
            var str2 = lookup[order[doc]];
            if (bottom == null)
                return str2 == null ? 0 : -1;
            if (str2 == null)
                return 1;

            return SafeNativeMethods.StrCmpLogicalW(bottom, str2);
        }

        public override void Copy(int slot, int doc)
        {
            values[slot] = lookup[order[doc]];
        }

        public override void SetNextReader(IndexReader reader, int docBase)
        {
            var currentReaderValues = FieldCache_Fields.DEFAULT.GetStringIndex(reader, field);
            order = currentReaderValues.order;
            lookup = currentReaderValues.lookup;
        }

        public override IComparable this[int slot] => values[slot];
    }
}
