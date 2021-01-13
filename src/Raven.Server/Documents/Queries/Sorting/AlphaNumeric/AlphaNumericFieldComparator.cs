using System;
using System.Collections.Generic;
using System.Text;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class AlphaNumericFieldComparator : FieldComparator
    {
        private readonly UnmanagedStringArray.UnmanagedString[] _values;
        private readonly string _field;
        private UnmanagedStringArray.UnmanagedString _bottom;
        private int[] _order;
        private UnmanagedStringArray _lookup;

        public AlphaNumericFieldComparator(string field, int numHits)
        {
            _values = new UnmanagedStringArray.UnmanagedString[numHits];
            _field = field;
        }

        public override int Compare(int slot1, int slot2)
        {
            var str1 = _values[slot1];
            var str2 = _values[slot2];

            if (str1.IsNull)
                return str2.IsNull ? 0 : -1;
            if (str2.IsNull)
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
            if (_bottom.IsNull)
                return str2.IsNull ? 0 : -1;
            if (str2.IsNull)
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
        internal sealed class AlphanumComparer : IComparer<UnmanagedStringArray.UnmanagedString>
        {
            public static readonly AlphanumComparer Instance = new AlphanumComparer();

            private AlphanumComparer()
            {

            }

            public unsafe struct AlphanumericStringComparisonState
            {
                public int CurPositionInString;
                public char CurCharacter;
                public readonly UnmanagedStringArray.UnmanagedString OriginalString;
                public readonly int StringLength;
                public bool CurSequenceIsNumber;
                public int NumberLength;
                public int CurSequenceStartPosition;
                public int StringBufferOffset;

                public AlphanumericStringComparisonState(UnmanagedStringArray.UnmanagedString originalString)
                {
                    StringLength = Encoding.UTF8.GetCharCount(originalString.StringAsBytes);
                    OriginalString = originalString;
                    CurSequenceStartPosition = 0;
                    NumberLength = 0;
                    CurSequenceIsNumber = false;
                    CurCharacter = (char)0;
                    CurPositionInString = 0;
                    StringBufferOffset = 0;
                }

                public void ScanNextAlphabeticOrNumericSequence()
                {
                    CurSequenceStartPosition = StringBufferOffset;
                    var used = ReadOneChar(OriginalString.StringAsBytes, StringBufferOffset, ref CurCharacter);
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
                        StringBufferOffset += used;

                        if (CurPositionInString < StringLength)
                        {
                            used = ReadOneChar(OriginalString.StringAsBytes, StringBufferOffset, ref CurCharacter);
                            curCharacterIsDigit = char.IsDigit(CurCharacter);
                        }
                        else
                        {
                            break;
                        }
                    } while (curCharacterIsDigit == CurSequenceIsNumber);

                }

                [ThreadStatic]
                private static Decoder Decoder;

                private static int ReadOneChar(Span<byte> str, int offset, ref char ch)
                {
                    fixed (byte* buffer = str)
                    fixed (char* c = &ch)
                    {
                        var decoder = Decoder ??= Encoding.UTF8.GetDecoder();
                        decoder.Convert(buffer + offset, str.Length - offset, c, 1, flush: true, out var bytesUsed,
                            out var charUsed, out _);

                        if (charUsed != 1)
                            throw new InvalidOperationException($"Read unexpected number of chars {charUsed} from string: '{Encoding.UTF8.GetString(str)}' at offset: {offset}");

                        return bytesUsed;
                    }
                }

                public int CompareWithAnotherState(AlphanumericStringComparisonState other)
                {
                    var string1State = this;
                    var string2State = other;

                    // if both sequences are numbers, compare between them
                    if (string1State.CurSequenceIsNumber && string2State.CurSequenceIsNumber)
                    {
                        // if effective numbers are not of the same length, it means that we can tell which is greater (in an order of magnitude, actually)
                        if (string1State.NumberLength != string2State.NumberLength)
                        {
                            return string1State.NumberLength.CompareTo(string2State.NumberLength);
                        }

                        // else, it means they should be compared by string, again, we compare only the effective numbers
                        // One digit is always one byte, so no need to care about chars vs bytes 
                        return string1State.OriginalString.StringAsBytes.Slice(string1State.StringBufferOffset - string1State.NumberLength, string1State.NumberLength)
                            .SequenceCompareTo(string2State.OriginalString.StringAsBytes.Slice(string2State.StringBufferOffset - string2State.NumberLength,
                                string2State.NumberLength));
                    }

                    // if one of the sequences is a number and the other is not, the number is always smaller
                    if (string1State.CurSequenceIsNumber != string2State.CurSequenceIsNumber)
                    {
                        if (string1State.CurSequenceIsNumber)
                            return -1;
                        return 1;
                    }

                    // should be case insensitive
                    char ch1 = default;
                    char ch2 = default;
                    var offset1 = string1State.CurSequenceStartPosition;
                    var offset2 = string2State.CurSequenceStartPosition;

                    var length1 = string1State.StringBufferOffset - string1State.CurSequenceStartPosition;
                    var length2 = string2State.StringBufferOffset - string2State.CurSequenceStartPosition;

                    while (length1 > 0 && length2 > 0)
                    {
                        var read1 = ReadOneChar(string1State.OriginalString.StringAsBytes, offset1, ref ch1);
                        var read2 = ReadOneChar(string2State.OriginalString.StringAsBytes, offset2, ref ch2);

                        length1 -= read1;
                        length2 -= read2;

                        var result = char.ToLowerInvariant(ch1) - char.ToLowerInvariant(ch2);

                        if (result == 0)
                        {
                            offset1 += read1;
                            offset2 += read2;
                            continue;
                        }

                        return result;
                    }

                    return length1 - length2;
                }
            }

            // Used for testing only
            public unsafe int Compare(string string1, string string2)
            {
                var length1 = (short)Encoding.UTF8.GetByteCount(string1);
                var buffer1 = new byte[length1 + sizeof(short)];
                Encoding.UTF8.GetBytes(string1, new Span<byte>(buffer1, sizeof(short), length1));

                var length2 = (short)Encoding.UTF8.GetByteCount(string2);
                var buffer2 = new byte[length2 + sizeof(short)];
                Encoding.UTF8.GetBytes(string2, new Span<byte>(buffer2, sizeof(short), length2));

                fixed (byte* b1 = buffer1)
                fixed (byte* b2 = buffer2)
                {
                    *(short*)b1 = length1;
                    *(short*)b2 = length2;

                    var us1 = new UnmanagedStringArray.UnmanagedString
                    {
                        Start = b1
                    };

                    var us2 = new UnmanagedStringArray.UnmanagedString
                    {
                        Start = b2
                    };

                    return Compare(us1, us2);
                }
            }

            public int Compare(UnmanagedStringArray.UnmanagedString string1, UnmanagedStringArray.UnmanagedString string2)
            {
                if (string1.IsNull)
                {
                    return 0;
                }

                if (string2.IsNull)
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
