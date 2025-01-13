using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;
using Constants = Raven.Client.Constants;
using NativeMemory = Sparrow.Utils.NativeMemory;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public sealed class AlphaNumericFieldComparator : FieldComparator
    {
        private readonly UnmanagedStringArray.UnmanagedString[] _values;
        private readonly string _field;
        private UnmanagedStringArray.UnmanagedString _bottom;
        private int[] _order;
        private UnmanagedStringArray _lookup;
        private static readonly UnmanagedStringArray.UnmanagedString NullValue = GetNullValueUnmanagedString();

        private static unsafe UnmanagedStringArray.UnmanagedString GetNullValueUnmanagedString()
        {
            var byteCount = Encoding.UTF8.GetByteCount(Constants.Documents.Indexing.Fields.NullValue);
            byte* bytes = NativeMemory.AllocateMemory(byteCount + sizeof(int)); // single allocation, we never free it
            fixed (char* chars = Constants.Documents.Indexing.Fields.NullValue)
            {
                *(int*)bytes = Encoding.UTF8.GetBytes(chars, Constants.Documents.Indexing.Fields.NullValue.Length,
                    bytes + sizeof(int), byteCount);

                *(int*)bytes = *(int*)bytes << 1 | 1;
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

        // based on: https://www.dotnetperls.com/alphanumeric-sorting
        internal abstract class AbstractAlphanumericComparisonState<T>
        {
            public readonly T OriginalString;
            private readonly int _stringLength;
            private bool _currentSequenceIsNumber;
            public int CurrentPositionInString;
            public int NumberLength;
            public int CurrentSequenceStartPosition;
            public int StringBufferOffset;

            protected AbstractAlphanumericComparisonState(T originalString, int stringLength)
            {
                OriginalString = originalString;
                _stringLength = stringLength;
                _currentSequenceIsNumber = false;
                CurrentPositionInString = 0;
                NumberLength = 0;
                CurrentSequenceStartPosition = 0;
                StringBufferOffset = 0;
            }

            protected abstract int GetStartPosition();

            protected abstract int ReadCharacter(int offset, Span<char> charactersBuffer);
            
            protected abstract int CompareNumbersAsStrings(AbstractAlphanumericComparisonState<T> string2State);

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

            private unsafe void ScanNextAlphabeticOrNumericSequence()
            {
                CurrentSequenceStartPosition = GetStartPosition();
                Span<char> characters = stackalloc char[4];
                var used = ReadCharacter(StringBufferOffset, characters);
                _currentSequenceIsNumber =  used == 1 && char.IsDigit(characters[0]);
                NumberLength = 0;

                bool currentCharacterIsDigit;
                var insideZeroPrefix = characters[0] == '0';

                // Walk through all following characters that are digits or
                // characters in BOTH strings starting at the appropriate marker.
                // Collect char arrays.
                do
                {
                    if (_currentSequenceIsNumber)
                    {
                        if (characters[0] != '0')
                        {
                            insideZeroPrefix = false;
                        }

                        if (insideZeroPrefix == false)
                        {
                            NumberLength++;
                        }
                    }

                    CurrentPositionInString += used;
                    StringBufferOffset += used;

                    if (CurrentPositionInString < _stringLength)
                    {
                        used = ReadCharacter(StringBufferOffset, characters);
                        currentCharacterIsDigit = used == 1 && char.IsDigit(characters[0]);
                    }
                    else
                    {
                        break;
                    }
                } while (currentCharacterIsDigit == _currentSequenceIsNumber);

            }

            private unsafe int CompareSequence(AbstractAlphanumericComparisonState<T> other)
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

                // should be case insensitive
                var offset1 = CurrentSequenceStartPosition;
                var offset2 = other.CurrentSequenceStartPosition;
                Span<char> ch1 = stackalloc char[4];
                Span<char> ch2 = stackalloc char[4];
                var length1 = StringBufferOffset - CurrentSequenceStartPosition;
                var length2 = other.StringBufferOffset - other.CurrentSequenceStartPosition;

                while (length1 > 0 && length2 > 0)
                {
                    var read1Chars = ReadCharacter(offset1, ch1);
                    var read2Chars = other.ReadCharacter(offset2, ch2);

                    length1 -= read1Chars;
                    length2 -= read2Chars;
                    
                    int result = read1Chars switch
                    {
                        1 when read2Chars == 1 => char.ToLowerInvariant(ch1[0]) - char.ToLowerInvariant(ch2[0]),
                        2 when read2Chars == 2 => ch1.Slice(0, read1Chars).SequenceCompareTo(ch2.Slice(0, read2Chars)),
                        1 => -1, //non-surroagate is always bigger than surrogate character
                        _ => 1
                    };

                    if (result == 0)
                    {
                        offset1 += read1Chars;
                        offset2 += read2Chars;
                        continue;
                    }
                    
                    return result;
                }

                return length1 - length2;
            }
        }

        internal sealed class StringAlphanumComparer : IComparer<string>
        {
            public static readonly StringAlphanumComparer Instance = new();

            private StringAlphanumComparer()
            {

            }

            private sealed class AlphanumericStringComparisonState : AbstractAlphanumericComparisonState<string>
            {
                public AlphanumericStringComparisonState(string originalString) : base(originalString, originalString.Length)
                {
                }

                protected override int GetStartPosition()
                {
                    return CurrentPositionInString;
                }

                protected override int ReadCharacter(int offset, Span<char> charactersBuffer)
                {
                    //in this overload we don't have bytearray so we've to treat bytes as chars
                    charactersBuffer[0] = OriginalString[offset];
                    
                    if (char.IsSurrogate(OriginalString[offset]) && offset + 1 < OriginalString.Length 
                                                                 && char.IsSurrogatePair(OriginalString, offset))
                    {
                        charactersBuffer[1] = OriginalString[offset + 1];
                        return 2;
                    }

                    return 1;
                }
                
                protected override int CompareNumbersAsStrings(AbstractAlphanumericComparisonState<string> other)
                {
                    return System.Globalization.CultureInfo.InvariantCulture.CompareInfo.Compare(
                        OriginalString, CurrentPositionInString - NumberLength, NumberLength,
                        other.OriginalString, other.CurrentPositionInString - other.NumberLength, other.NumberLength);
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

        internal sealed class UnmanagedStringAlphanumComparer : IComparer<UnmanagedStringArray.UnmanagedString>
        {
            public static readonly UnmanagedStringAlphanumComparer Instance = new();

            private UnmanagedStringAlphanumComparer()
            {

            }

            private sealed class AlphanumericUnmanagedStringAsBytesComparisonState : AbstractAlphanumericComparisonState<UnmanagedStringArray.UnmanagedString>
            {
                public AlphanumericUnmanagedStringAsBytesComparisonState(UnmanagedStringArray.UnmanagedString originalString)
                    : base(originalString, originalString.StringAsBytes.Length)
                {
                    Debug.Assert(OriginalString.StoredAsAscii);
                }

                protected override int GetStartPosition()
                {
                    return StringBufferOffset;
                }

                protected override int ReadCharacter(int offset, Span<char> charactersBuffer)
                {
                    var spanByte = OriginalString.StringAsBytes.Slice(offset, 1);
                    charactersBuffer[0] = (char)spanByte[0];
                    return 1;
                }

                protected override int CompareNumbersAsStrings(AbstractAlphanumericComparisonState<UnmanagedStringArray.UnmanagedString> other)
                {
                    return OriginalString.StringAsBytes.Slice(StringBufferOffset - NumberLength, NumberLength)
                        .SequenceCompareTo(other.OriginalString.StringAsBytes.Slice(other.StringBufferOffset - other.NumberLength,
                            other.NumberLength));
                }
            }

            private sealed class AlphanumericUnmanagedStringAsCharsComparisonState : AbstractAlphanumericComparisonState<UnmanagedStringArray.UnmanagedString>
            {
                public AlphanumericUnmanagedStringAsCharsComparisonState(UnmanagedStringArray.UnmanagedString originalString)
                    : base(originalString, originalString.StringAsChars.Length)
                {
                    Debug.Assert(OriginalString.StoredAsAscii == false);
                }

                protected override int GetStartPosition()
                {
                    return StringBufferOffset;
                }

                protected override int ReadCharacter(int offset, Span<char> charactersBuffer)
                {
                    var ch0 = charactersBuffer[0] = OriginalString.StringAsChars[offset];

                    if (char.IsSurrogate(ch0) 
                        && offset + 1 < OriginalString.StringAsChars.Length
                        && char.IsSurrogatePair(ch0, OriginalString.StringAsChars[offset + 1]))
                    {
                        Debug.Assert(charactersBuffer.Length >= 2, "charactersBuffer must have at least 2 slots for potential surrogate pairs.");
                        charactersBuffer[1] = OriginalString.StringAsChars[offset + 1];
                        return 2;
                    }

                    return 1;
                }
                
                protected override int CompareNumbersAsStrings(AbstractAlphanumericComparisonState<UnmanagedStringArray.UnmanagedString> other)
                {
                    return OriginalString.StringAsChars.Slice(StringBufferOffset - NumberLength, NumberLength)
                        .SequenceCompareTo(other.OriginalString.StringAsChars.Slice(other.StringBufferOffset - other.NumberLength, other.NumberLength));
                }
            }

            // Used for testing only
            public unsafe int Compare(string string1, string string2)
            {
                var us1 = new UnmanagedStringArray.UnmanagedString
                {
                    Start = Allocate(string1)
                };

                var us2 = new UnmanagedStringArray.UnmanagedString
                {
                    Start = Allocate(string2)
                };

                return Compare(us1, us2);
            }

            // Used for testing only
            private static unsafe byte* Allocate(string str)
            {
                var sizeForBytes = str.Length + sizeof(int);  // assume 1 byte per char for ascii
                var buffer = (byte*)Marshal.AllocHGlobal((IntPtr)sizeForBytes);
                var outputByteBuffer = new Span<byte>(buffer + sizeof(int), str.Length);
                if (Encoding.UTF8.TryGetBytes(str, outputByteBuffer, out var bytesWritten))
                {
                    *((int*)buffer) = bytesWritten;
                    *((int*)buffer) = bytesWritten << 1 | 1;
                }
                else
                {
                    var sizeForChars = str.Length * sizeof(char) + sizeof(int);
                    buffer = (byte*)Marshal.AllocHGlobal((IntPtr)sizeForChars);
                    var outputCharBuffer = new Span<char>(buffer + sizeof(int), str.Length);
                    str.CopyTo(outputCharBuffer);

                    *((int*)buffer) = str.Length;
                    *((int*)buffer) = str.Length << 1 | 0;
                }

                return buffer;
            }

            public int Compare(UnmanagedStringArray.UnmanagedString string1, UnmanagedStringArray.UnmanagedString string2)
            {
                Debug.Assert(string1.IsNull == false);
                Debug.Assert(string2.IsNull == false);

                var string1State = string1.StoredAsAscii ? (AbstractAlphanumericComparisonState<UnmanagedStringArray.UnmanagedString>)new AlphanumericUnmanagedStringAsBytesComparisonState(string1) : new AlphanumericUnmanagedStringAsCharsComparisonState(string1);
                var string2State = string2.StoredAsAscii ? (AbstractAlphanumericComparisonState<UnmanagedStringArray.UnmanagedString>)new AlphanumericUnmanagedStringAsBytesComparisonState(string2) : new AlphanumericUnmanagedStringAsCharsComparisonState(string2);

                return string1State.CompareTo(string2State);
            }
        }
    }
}
