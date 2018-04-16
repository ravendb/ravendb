using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class AlphaNumericFieldComparator : FieldComparator
    {
        private readonly string[] _values;
        private readonly string _field;
        private string _bottom;
        private int[] _order;
        private string[] _lookup;

        public AlphaNumericFieldComparator(string field, string[] valuesArray)
        {
            _values = valuesArray;
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

        public override int CompareBottom(int doc, IState state)
        {
            var str2 = _lookup[_order[doc]];
            if (_bottom == null)
                return str2 == null ? 0 : -1;
            if (str2 == null)
                return 1;

            return AlphanumComparer.Instance.Compare(_bottom, str2);
        }

        public override void Copy(int slot, int doc, IState state)
        {
            _values[slot] = _lookup[_order[doc]];
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            var currentReaderValues = FieldCache_Fields.DEFAULT.GetStringIndex(reader, _field, state);
            _order = currentReaderValues.order;
            _lookup = currentReaderValues.lookup;
        }

        public override IComparable this[int slot] => _values[slot];

        // based on: https://www.dotnetperls.com/alphanumeric-sorting
        internal sealed class AlphanumComparer : IComparer<string>
        {
            public static readonly AlphanumComparer Instance = new AlphanumComparer();

            private AlphanumComparer()
            {

            }

            public struct AlphanumericStringComparisonState
            {
                public int CurPositionInString;
                public char CurCharacter;
                public readonly string OriginalString;
                public readonly int StringLength;
                public bool CurSequenceIsNumber;
                public int NumberLength;
                public int CurSequenceStartPosition;

                public AlphanumericStringComparisonState(string originalString)
                {
                    OriginalString = originalString;
                    StringLength = originalString.Length;
                    CurSequenceStartPosition = 0;
                    NumberLength = 0;
                    CurSequenceIsNumber = false;
                    CurCharacter = (char)0;
                    CurPositionInString = 0;
                }

                public void ScanNextAlphabeticOrNumericSequence()
                {
                    CurSequenceStartPosition = CurPositionInString;
                    CurCharacter = OriginalString[CurPositionInString];
                    CurSequenceIsNumber = char.IsDigit(CurCharacter);
                    NumberLength = 0;

                    var curCharacterIsDigit = CurSequenceIsNumber;
                    var insideZeroPrefix = CurCharacter == '0';

                    // Walk through all following characters that are digits or
                    // characters in BOTH strings starting at the appropriate marker.
                    // Collect char arrays.
                    do
                    {
                        if (CurSequenceIsNumber)
                        {
                            if (CurCharacter != '0')
                            {
                                insideZeroPrefix = false;
                            }

                            if (insideZeroPrefix == false)
                            {
                                NumberLength++;
                            }
                        }

                        CurPositionInString++;

                        if (CurPositionInString < StringLength)
                        {
                            CurCharacter = OriginalString[CurPositionInString];
                            curCharacterIsDigit = char.IsDigit(CurCharacter);
                        }
                        else
                        {
                            break;
                        }
                    } while (curCharacterIsDigit == CurSequenceIsNumber);

                }

                public int CompareWithAnotherState(AlphanumericStringComparisonState other)
                {
                    var string1State = this;
                    var string2State = other;

                    // if both seqeunces are numbers, compare between them
                    if (string1State.CurSequenceIsNumber && string2State.CurSequenceIsNumber)
                    {
                        // if effective numbers are not of the same length, it means that we can tell which is greatedr (in an order of magnitude, actually)
                        if (string1State.NumberLength != string2State.NumberLength)
                        {
                            return string1State.NumberLength.CompareTo(string2State.NumberLength);
                        }

                        // else, it means they should be compared by string, again, we compare only the effective numbers
                        return System.Globalization.CultureInfo.InvariantCulture.CompareInfo.Compare(
                            string1State.OriginalString, string1State.CurPositionInString - string1State.NumberLength, string1State.NumberLength,
                            string2State.OriginalString, string2State.CurPositionInString - string2State.NumberLength, string1State.NumberLength);
                    }

                    // if one of the sequences is a number and the other is not, the number is always smaller
                    if (string1State.CurSequenceIsNumber != string2State.CurSequenceIsNumber)
                    {
                        if (string1State.CurSequenceIsNumber)
                            return -1;
                        return 1;
                    }

                    return System.Globalization.CultureInfo.InvariantCulture.CompareInfo.Compare(
                    string1State.OriginalString, string1State.CurSequenceStartPosition, string1State.CurPositionInString - string1State.CurSequenceStartPosition,
                    string2State.OriginalString, string2State.CurSequenceStartPosition, string2State.CurPositionInString - string2State.CurSequenceStartPosition);
                }
            }

            public unsafe int Compare(string string1, string string2)
            {
                if (string1 == null)
                {
                    return 0;
                }

                if (string2 == null)
                {
                    return 0;
                }

                var string1State = new AlphanumericStringComparisonState(string1);
                var string2State = new AlphanumericStringComparisonState(string2);

                // Walk through two the strings with two markers.
                while (string1State.CurPositionInString < string1State.StringLength &&
                        string2State.CurPositionInString < string2State.StringLength)
                {
                    string1State.ScanNextAlphabeticOrNumericSequence();
                    string2State.ScanNextAlphabeticOrNumericSequence();

                    var result = string1State.CompareWithAnotherState(string2State);

                    if (result != 0)
                    {
                        return result;
                    }
                }

                if (string1State.CurPositionInString < string1State.StringLength)
                    return 1;
                if (string2State.CurPositionInString < string2State.StringLength)
                    return -1;

                return 0;
            }

        }
    }
}
