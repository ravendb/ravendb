using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
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

            return AlphanumComparer.Instance.Compare(str1, str2);
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
                public int CurrentCharacterCharAmount;
                public fixed char CurCharacters[4];
                public int CurPositionInString;
                public readonly UnmanagedStringArray.UnmanagedString OriginalString;
                public readonly int StringLength;
                public bool CurSequenceIsNumber;
                public int NumberLength;
                public int CurSequenceStartPosition;
                public int StringBufferOffset;

                public unsafe AlphanumericStringComparisonState(UnmanagedStringArray.UnmanagedString originalString)
                {
                    StringLength = Encoding.UTF8.GetCharCount(originalString.StringAsBytes);
                    OriginalString = originalString;
                    CurSequenceStartPosition = 0;
                    NumberLength = 0;
                    CurSequenceIsNumber = false;
                    CurPositionInString = 0;
                    StringBufferOffset = 0;
                    CurrentCharacterCharAmount = 0;
                }

                public void ScanNextAlphabeticOrNumericSequence()
                {
                    fixed (char* ptrCurCharacters = CurCharacters)
                    {
                        CurSequenceStartPosition = StringBufferOffset;
                        var characterBuffer = new Span<char>(ptrCurCharacters, 4);
                        var (usedBytes, usedChars) = ReadCharacter(OriginalString.StringAsBytes, StringBufferOffset, characterBuffer);
                        CurSequenceIsNumber = usedChars == 1 && char.IsDigit(CurCharacters[0]);
                        NumberLength = 0;

                        var curCharacterIsDigit = CurSequenceIsNumber;
                        var insideZeroPrefix = CurSequenceIsNumber && CurCharacters[0] == '0';

                        // Walk through all following characters that are digits or
                        // characters in BOTH strings starting at the appropriate marker.
                        // Collect char arrays.
                        do
                        {
                            if (CurSequenceIsNumber)
                            {
                                if (CurCharacters[0] != '0')
                                {
                                    insideZeroPrefix = false;
                                }

                                if (insideZeroPrefix == false)
                                {
                                    NumberLength++;
                                }
                            }

                            CurPositionInString += usedChars;
                            StringBufferOffset += usedBytes;

                            if (CurPositionInString < StringLength)
                            {
                                (usedBytes, usedChars) = ReadCharacter(OriginalString.StringAsBytes, StringBufferOffset, characterBuffer);
                                curCharacterIsDigit = usedChars == 1 && char.IsDigit(CurCharacters[0]);
                            }
                            else
                            {
                                break;
                            }
                        } while (curCharacterIsDigit == CurSequenceIsNumber);
                    }
                }

                [ThreadStatic]
                private static Decoder Decoder;

                private static (int BytesUsed, int CharUsed) ReadCharacter(ReadOnlySpan<byte> str, int offset, Span<char> charactersBuffer)
                {
                    var decoder = Decoder ??= Encoding.UTF8.GetDecoder();
                    
                    //Numbers and ASCII are always 1 so we will pay the price only in case of UTF-8 characters.
                    //http://www.unicode.org/versions/Unicode9.0.0/ch03.pdf#page=54
                    var (byteLengthOfCharacter, charNeededToEncodeCharacters) = str[offset] switch
                    {
                        <= 0b0111_1111 => (1,1), /* 1 byte sequence: 0b0xxxxxxxx */
                        <= 0b1101_1111 => (2, Encoding.UTF8.GetCharCount(str.Slice(offset, 2))), /* 2 byte sequence: 0b110xxxxxx */
                        <= 0b1110_1111 => (3, Encoding.UTF8.GetCharCount(str.Slice(offset, 3))), /* 0b1110xxxx: 3 bytes sequence */
                        <= 0b1111_0111 => (4, Encoding.UTF8.GetCharCount(str.Slice(offset, 4))), /* 0b11110xxx: 4 bytes sequence */
                        _ => throw new InvalidDataException($"Characters should be between 1 and 4 bytes long and cannot match the specified sequence. This is invalid code.")
                    };
                    
                    Debug.Assert(charactersBuffer.Length >= charNeededToEncodeCharacters, $"Character requires more than {charactersBuffer.Length} space to be decoded.");
                    
                    //In case of surrogate we could've to use two characters to convert it.
                    decoder.Convert(str.Slice(offset, byteLengthOfCharacter), charactersBuffer, flush: true, out int bytesUsed,out int charUsed, out _);
                    
                    return (bytesUsed, charUsed);
                }

                public int CompareWithAnotherState(ref AlphanumericStringComparisonState other)
                {
                    ref var string1State = ref this;
                    ref var string2State = ref other;

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
                    Span<char> ch1 = stackalloc char[4];
                    Span<char> ch2 = stackalloc char[4];
                    var offset1 = string1State.CurSequenceStartPosition;
                    var offset2 = string2State.CurSequenceStartPosition;

                    var length1 = string1State.StringBufferOffset - string1State.CurSequenceStartPosition;
                    var length2 = string2State.StringBufferOffset - string2State.CurSequenceStartPosition;

                    while (length1 > 0 && length2 > 0)
                    {
                        var (read1Bytes, read1Chars) = ReadCharacter(string1State.OriginalString.StringAsBytes, offset1, ch1);
                        var (read2Bytes, read2Chars) = ReadCharacter(string2State.OriginalString.StringAsBytes, offset2, ch2);

                        length1 -= read1Bytes;
                        length2 -= read2Bytes;

                        int result = read1Chars switch
                        {
                            1 when read2Chars == 1 => char.ToLowerInvariant(ch1[0]) - char.ToLowerInvariant(ch2[0]),
                            2 when read2Chars == 2 => ch1.Slice(0, read1Chars).SequenceCompareTo(ch2.Slice(0, read2Chars)),
                            1 => -1, //non-surroagate is always bigger than surrogate character
                            _ => 1
                        };

                        if (result == 0)
                        {
                            offset1 += read1Bytes;
                            offset2 += read2Bytes;
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
                Debug.Assert(string1.IsNull == false);
                Debug.Assert(string2.IsNull == false);
                
                var string1State = new AlphanumericStringComparisonState(string1);
                var string2State = new AlphanumericStringComparisonState(string2);

                // Walk through two the strings with two markers.
                while (string1State.CurPositionInString < string1State.StringLength &&
                        string2State.CurPositionInString < string2State.StringLength)
                {
                    string1State.ScanNextAlphabeticOrNumericSequence();
                    string2State.ScanNextAlphabeticOrNumericSequence();

                    var result = string1State.CompareWithAnotherState(ref string2State);

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
