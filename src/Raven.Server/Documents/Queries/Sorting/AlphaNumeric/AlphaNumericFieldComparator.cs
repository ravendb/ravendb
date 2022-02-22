using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Constants = Raven.Client.Constants;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class AlphaNumericFieldComparator : FieldComparator
    {
        private readonly UnmanagedStringArray.UnmanagedString[] _values;
        private readonly string _field;
        private UnmanagedStringArray.UnmanagedString _bottom;
        private int[] _order;
        private UnmanagedStringArray _lookup;
        private static readonly UnmanagedStringArray.UnmanagedString NullValue = GetNullValueUnmanagedString();

        private static unsafe UnmanagedStringArray.UnmanagedString GetNullValueUnmanagedString()
        {
            var size = sizeof(short) + Encoding.UTF8.GetByteCount(Constants.Documents.Indexing.Fields.NullValue);
            byte* bytes = NativeMemory.AllocateMemory(size); // single allocation, we never free it
            fixed (char* chars = Constants.Documents.Indexing.Fields.NullValue)
            {
                *(short*)bytes = (short)Encoding.UTF8.GetBytes(chars, Constants.Documents.Indexing.Fields.NullValue.Length,
                    bytes + sizeof(short), size - sizeof(short));
            }
            
            return new UnmanagedStringArray.UnmanagedString
            {
                Start = bytes
            };
        }

        public AlphaNumericFieldComparator(string field, int numHits)
        {
            _values = new UnmanagedStringArray.UnmanagedString[numHits];
            _field = field;
        }

        public override int Compare(int slot1, int slot2)
        {
            var str1 = _values[slot1];
            var str2 = _values[slot2];

            if (IsNull(str1))
                return IsNull(str2) ? 0 : -1;
            if (IsNull(str2))
                return 1;

            return UnmanagedStringAlphanumComparer.Instance.Compare(str1, str2);
        }

        private static bool IsNull(UnmanagedStringArray.UnmanagedString str1)
        {
            return str1.IsNull|| UnmanagedStringArray.UnmanagedString.CompareOrdinal(str1, NullValue) == 0;
        }

        public override void SetBottom(int slot)
        {
            _bottom = _values[slot];
        }

        public override int CompareBottom(int doc, IState state)
        {
            var str2 = _lookup[_order[doc]];
            if (IsNull(_bottom))
                return IsNull(str2) ? 0 : -1;
            if (IsNull(str2))
                return 1;

            return UnmanagedStringAlphanumComparer.Instance.Compare(_bottom, str2);
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


        internal abstract class AbstractAlphanumericComparisonState<T>
        {
            public readonly T OriginalString;
            private readonly int _stringLength;
            private bool _currentSequenceIsNumber;
            public int CurrentPositionInString;
            public char CurrentCharacter;
            public int NumberLength;
            public int CurrentSequenceStartPosition;
            public int StringBufferOffset;

            protected AbstractAlphanumericComparisonState(T originalString, int stringLength)
            {
                OriginalString = originalString;
                _stringLength = stringLength;
                _currentSequenceIsNumber = false;
                CurrentPositionInString = 0;
                CurrentCharacter = (char)0;
                NumberLength = 0;
                CurrentSequenceStartPosition = 0;
                StringBufferOffset = 0;
            }

            protected abstract int GetStartPosition();

            protected abstract char ReadOneChar(out int bytesUsed);

            protected abstract int CompareNumbersAsStrings(AbstractAlphanumericComparisonState<T> string2State);

            protected abstract int CompareStrings(AbstractAlphanumericComparisonState<T> string2State);

            public int CompareTo(AbstractAlphanumericComparisonState<T> other)
            {
                // Walk through two the strings with two markers.
                while (CurrentPositionInString < _stringLength &&
                       other.CurrentPositionInString < other._stringLength)
                {
                    ScanNextAlphabeticOrNumericSequence();
                    other.ScanNextAlphabeticOrNumericSequence();

                    var result = CompareSequence(other);

                    if (result != 0)
                    {
                        return result;
                    }
                }

                if (CurrentPositionInString < _stringLength)
                    return 1;

                if (other.CurrentPositionInString < other._stringLength)
                    return -1;

                return 0;
            }

            private void ScanNextAlphabeticOrNumericSequence()
            {
                CurrentSequenceStartPosition = GetStartPosition();
                CurrentCharacter = ReadOneChar(out var bytesUsed);
                _currentSequenceIsNumber = char.IsDigit(CurrentCharacter);
                NumberLength = 0;

                bool currentCharacterIsDigit;
                var insideZeroPrefix = CurrentCharacter == '0';

                // Walk through all following characters that are digits or
                // characters in BOTH strings starting at the appropriate marker.
                // Collect char arrays.
                do
                {
                    if (_currentSequenceIsNumber)
                    {
                        if (CurrentCharacter != '0')
                        {
                            insideZeroPrefix = false;
                        }

                        if (insideZeroPrefix == false)
                        {
                            NumberLength++;
                        }
                    }

                    CurrentPositionInString++;
                    StringBufferOffset += bytesUsed;

                    if (CurrentPositionInString < _stringLength)
                    {
                        CurrentCharacter = ReadOneChar(out bytesUsed);
                        currentCharacterIsDigit = char.IsDigit(CurrentCharacter);
                    }
                    else
                    {
                        break;
                    }
                } while (currentCharacterIsDigit == _currentSequenceIsNumber);

            }

            private int CompareSequence(AbstractAlphanumericComparisonState<T> other)
            {
                // if both sequences are numbers, compare between them
                if (_currentSequenceIsNumber && other._currentSequenceIsNumber)
                {
                    // if effective numbers are not of the same length, it means that we can tell which is greater (in an order of magnitude, actually)
                    if (NumberLength != other.NumberLength)
                    {
                        return NumberLength.CompareTo(other.NumberLength);
                    }

                    // else, it means they should be compared by string, again, we compare only the effective numbers
                    return CompareNumbersAsStrings(other);
                    
                }

                // if one of the sequences is a number and the other is not, the number is always smaller
                if (_currentSequenceIsNumber != other._currentSequenceIsNumber)
                {
                    if (_currentSequenceIsNumber)
                        return -1;

                    return 1;
                }

                return CompareStrings(other);
            }
        }

        // based on: https://www.dotnetperls.com/alphanumeric-sorting
        internal sealed class StringAlphanumComparer : IComparer<string>
        {
            public static readonly StringAlphanumComparer Instance = new StringAlphanumComparer();

            private StringAlphanumComparer()
            {

            }

            private class AlphanumericStringComparisonState : AbstractAlphanumericComparisonState<string>
            {
                public AlphanumericStringComparisonState(string originalString) : base(originalString, originalString.Length)
                {
                }

                protected override int GetStartPosition()
                {
                    return CurrentPositionInString;
                }

                protected override char ReadOneChar(out int bytesUsed)
                {
                    bytesUsed = 0; // irrelevant
                    return OriginalString[CurrentPositionInString];
                }

                protected override int CompareNumbersAsStrings(AbstractAlphanumericComparisonState<string> other)
                {
                    return System.Globalization.CultureInfo.InvariantCulture.CompareInfo.Compare(
                        OriginalString, CurrentPositionInString - NumberLength, NumberLength,
                        other.OriginalString, other.CurrentPositionInString - other.NumberLength, other.NumberLength);
                }

                protected override int CompareStrings(AbstractAlphanumericComparisonState<string> other)
                {
                    return System.Globalization.CultureInfo.InvariantCulture.CompareInfo.Compare(
                        OriginalString, CurrentSequenceStartPosition, CurrentPositionInString - CurrentSequenceStartPosition,
                        other.OriginalString, other.CurrentSequenceStartPosition, other.CurrentPositionInString - other.CurrentSequenceStartPosition);
                }
            }

            public int Compare(string string1, string string2)
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

                return string1State.CompareTo(string2State);
            }
        }

        // based on: https://www.dotnetperls.com/alphanumeric-sorting
        internal sealed class UnmanagedStringAlphanumComparer : IComparer<UnmanagedStringArray.UnmanagedString>
        {
            public static readonly UnmanagedStringAlphanumComparer Instance = new UnmanagedStringAlphanumComparer();

            private UnmanagedStringAlphanumComparer()
            {

            }

            private class AlphanumericUnmanagedStringComparisonState : AbstractAlphanumericComparisonState<UnmanagedStringArray.UnmanagedString>
            {
                [ThreadStatic]
                private static Decoder Decoder;

                public AlphanumericUnmanagedStringComparisonState(UnmanagedStringArray.UnmanagedString originalString)
                    : base(originalString, Encoding.UTF8.GetCharCount(originalString.StringAsBytes))
                {
                }

                protected override int GetStartPosition()
                {
                    return StringBufferOffset;
                }

                protected override char ReadOneChar(out int bytesUsed)
                {
                    bytesUsed = ReadOneChar(OriginalString.StringAsBytes, StringBufferOffset, ref CurrentCharacter);
                    return CurrentCharacter;
                }

                protected override int CompareNumbersAsStrings(AbstractAlphanumericComparisonState<UnmanagedStringArray.UnmanagedString> other)
                {
                    return OriginalString.StringAsBytes.Slice(StringBufferOffset - NumberLength, NumberLength)
                        .SequenceCompareTo(other.OriginalString.StringAsBytes.Slice(other.StringBufferOffset - other.NumberLength,
                            other.NumberLength));
                }

                protected override int CompareStrings(AbstractAlphanumericComparisonState<UnmanagedStringArray.UnmanagedString> other)
                {
                    // should be case insensitive
                    char ch1 = default;
                    char ch2 = default;
                    var offset1 = CurrentSequenceStartPosition;
                    var offset2 = other.CurrentSequenceStartPosition;

                    var length1 = StringBufferOffset - CurrentSequenceStartPosition;
                    var length2 = other.StringBufferOffset - other.CurrentSequenceStartPosition;

                    while (length1 > 0 && length2 > 0)
                    {
                        var read1 = ReadOneChar(OriginalString.StringAsBytes, offset1, ref ch1);
                        var read2 = ReadOneChar(other.OriginalString.StringAsBytes, offset2, ref ch2);

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

                private static unsafe int ReadOneChar(Span<byte> str, int offset, ref char ch)
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
                Debug.Assert(string1.IsNull == false);
                Debug.Assert(string2.IsNull == false);

                var string1State = new AlphanumericUnmanagedStringComparisonState(string1);
                var string2State = new AlphanumericUnmanagedStringComparisonState(string2);

                return string1State.CompareTo(string2State);
            }
        }
    }
}
