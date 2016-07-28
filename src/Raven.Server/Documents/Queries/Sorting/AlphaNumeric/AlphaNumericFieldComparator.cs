using System;
using System.Collections.Generic;

using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class AlphaNumericFieldComparator : FieldComparator
    {
        private readonly string[] _values;
        private readonly string _field;
        private string _bottom;
        private int[] _order;
        private string[] _lookup;

        public AlphaNumericFieldComparator(int numHits, string field)
        {
            _values = new string[numHits];
            _field = field;
        }

        public override int Compare(int slot1, int slot2)
        {
            var str1 = _values[slot1];
            var str2 = _values[slot2];

            if (str1 == null)
                return str2 == null ? 0 : -1;
            if (str2 == null)
                return 1;

            return AlphanumComparer.Instance.Compare(str1, str2);
        }

        public override void SetBottom(int slot)
        {
            _bottom = _values[slot];
        }

        public override int CompareBottom(int doc)
        {
            var str2 = _lookup[_order[doc]];
            if (_bottom == null)
                return str2 == null ? 0 : -1;
            if (str2 == null)
                return 1;

            return AlphanumComparer.Instance.Compare(_bottom, str2);
        }

        public override void Copy(int slot, int doc)
        {
            _values[slot] = _lookup[_order[doc]];
        }

        public override void SetNextReader(IndexReader reader, int docBase)
        {
            var currentReaderValues = FieldCache_Fields.DEFAULT.GetStringIndex(reader, _field);
            _order = currentReaderValues.order;
            _lookup = currentReaderValues.lookup;
        }

        public override IComparable this[int slot] => _values[slot];

        internal class AlphanumComparer : IComparer<string>
        {
            public static AlphanumComparer Instance = new AlphanumComparer();

            private AlphanumComparer()
            {
            }

            public int Compare(string s1, string s2)
            {
                if (s1 == null)
                {
                    return 0;
                }
                if (s2 == null)
                {
                    return 0;
                }

                var len1 = s1.Length;
                var len2 = s2.Length;
                var marker1 = 0;
                var marker2 = 0;

                // Walk through two the strings with two markers.
                while (marker1 < len1 && marker2 < len2)
                {
                    var ch1 = s1[marker1];
                    var ch2 = s2[marker2];

                    // Some buffers we can build up characters in for each chunk.
                    var space1 = new char[len1];
                    var loc1 = 0;
                    var space2 = new char[len2];
                    var loc2 = 0;

                    // Walk through all following characters that are digits or
                    // characters in BOTH strings starting at the appropriate marker.
                    // Collect char arrays.
                    do
                    {
                        space1[loc1++] = ch1;
                        marker1++;

                        if (marker1 < len1)
                        {
                            ch1 = s1[marker1];
                        }
                        else
                        {
                            break;
                        }
                    } while (char.IsDigit(ch1) == char.IsDigit(space1[0]));

                    do
                    {
                        space2[loc2++] = ch2;
                        marker2++;

                        if (marker2 < len2)
                        {
                            ch2 = s2[marker2];
                        }
                        else
                        {
                            break;
                        }
                    } while (char.IsDigit(ch2) == char.IsDigit(space2[0]));

                    // If we have collected numbers, compare them numerically.
                    // Otherwise, if we have strings, compare them alphabetically.
                    var str1 = new string(space1);
                    var str2 = new string(space2);

                    int result;

                    if (char.IsDigit(space1[0]) && char.IsDigit(space2[0]))
                    {
                        var thisNumericChunk = int.Parse(str1);
                        var thatNumericChunk = int.Parse(str2);
                        result = thisNumericChunk.CompareTo(thatNumericChunk);
                    }
                    else
                    {
                        result = str1.CompareTo(str2);
                    }

                    if (result != 0)
                    {
                        return result;
                    }
                }
                return len1 - len2;
            }
        }
    }
}
