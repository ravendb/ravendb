using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Corax.Querying.Matches.SortingMatches;

// This is copy-pasted from Raven.Server.Documents.Queries.Sorting.AlphaNumeric.AlphaNumericFieldComparator and adjusted for Corax input.
internal sealed class AlphanumericalComparer
{
    public static readonly AlphanumericalComparer Instance = new AlphanumericalComparer();

    private AlphanumericalComparer()
    {
    }

    private unsafe ref struct AlphanumericStringComparisonState
    {
        private readonly Span<char> _curCharacters;
        public uint CurrentBytePosition = 0;
        private readonly ReadOnlySpan<byte> _originalString;
        public readonly uint StringLengthInBytes;
        private bool _curSequenceIsNumber = false;
        private uint _numberLength = 0;
        private uint _numberLengthInBytes = 0;
        private uint _curSequenceStartPosition = 0;

        public void ScanNextAlphabeticOrNumericSequence()
        {
            _curSequenceStartPosition = CurrentBytePosition;
            var (usedBytes, usedChars) = ReadCharacter(_originalString, CurrentBytePosition, _curCharacters);
            _curSequenceIsNumber = usedChars == 1 && char.IsDigit(_curCharacters[0]);
            _numberLength = 0;
            _numberLengthInBytes = 0;

            var curCharacterIsDigit = _curSequenceIsNumber;
            var insideZeroPrefix = _curSequenceIsNumber && _curCharacters[0] == '0';

            // Walk through all following characters that are digits or
            // characters in BOTH strings starting at the appropriate marker.
            // Collect char arrays.
            do
            {
                if (_curSequenceIsNumber)
                {

                    if (_curCharacters[0] != '0')
                    {
                        insideZeroPrefix = false;
                    }

                    if (insideZeroPrefix == false)
                    {
                        _numberLength++;
                        _numberLengthInBytes += usedBytes;
                    }
                }

                CurrentBytePosition += usedBytes;

                if (CurrentBytePosition < StringLengthInBytes)
                {
                    (usedBytes, usedChars) = ReadCharacter(_originalString, CurrentBytePosition, _curCharacters);
                    curCharacterIsDigit = usedChars == 1 && char.IsDigit(_curCharacters[0]);
                }
                else
                {
                    break;
                }
            } while (curCharacterIsDigit == _curSequenceIsNumber);
        }

        [ThreadStatic]
        private static Decoder Decoder;

        public AlphanumericStringComparisonState(ReadOnlySpan<byte> originalString, Span<char> curCharacters)
        {
            _curCharacters = curCharacters;
            _originalString = originalString;
            StringLengthInBytes = (uint)originalString.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static (uint BytesUsed, uint CharUsed) ReadCharacter(ReadOnlySpan<byte> str, uint offset, Span<char> charactersBuffer)
        {
            var firstByte = Unsafe.Add(ref MemoryMarshal.GetReference(str), offset);
            if ((uint)firstByte <= 0b0111_1111)
            {
                charactersBuffer[0] = (char)firstByte;
                return (1, 1);
            }

            uint bytesUsed = ReadCharacterUtf8(firstByte, str, offset, charactersBuffer, out uint charUsed);
            return (bytesUsed, charUsed);
        }

        private static uint ReadCharacterUtf8(byte firstByte, ReadOnlySpan<byte> str, uint offset, Span<char> charactersBuffer, out uint charUsed)
        {
            var decoder = Decoder ??= Encoding.UTF8.GetDecoder();

            //Numbers and ASCII are always 1 so we will pay the price only in case of UTF-8 characters.
            //http://www.unicode.org/versions/Unicode9.0.0/ch03.pdf#page=54
            var (byteLengthOfCharacter, charNeededToEncodeCharacters) = firstByte switch
            {
                <= 0b0111_1111 => (1, 1), /* 1 byte sequence: 0b0xxxxxxxx */
                <= 0b1101_1111 => (2, 1), /* 2 byte sequence: 0b110xxxxxx */
                <= 0b1110_1111 => (3, 1), /* 0b1110xxxx: 3 bytes sequence */
                <= 0b1111_0111 => (4, 2), /* 0b11110xxx: 4 bytes sequence */
                _ => throw new InvalidDataException($"Characters should be between 1 and 4 bytes long and cannot match the specified sequence. This is invalid code.")
            };

            Debug.Assert(4 >= charNeededToEncodeCharacters, $"Character requires more than  space to be decoded.");

            //In case of surrogate we could've to use two characters to convert it.
            decoder.Convert(str.Slice((int)offset, byteLengthOfCharacter), charactersBuffer, flush: true, out int bytesUsed, out var charUsedInt, out _);
            charUsed = (uint)charUsedInt;
            return (uint)bytesUsed;
        }

        public int CompareWithAnotherState(ref AlphanumericStringComparisonState other)
        {
            ref var string1State = ref this;
            ref var string2State = ref other;

            // if both sequences are numbers, compare between them
            if (string1State._curSequenceIsNumber && string2State._curSequenceIsNumber)
            {
                // if effective numbers are not of the same length, it means that we can tell which is greater (in an order of magnitude, actually)
                if (string1State._numberLength != string2State._numberLength)
                {
                    return string1State._numberLength.CompareTo(string2State._numberLength);
                }

                if (string1State._numberLengthInBytes != 1 || string2State._numberLengthInBytes != 1)
                {
                    var str1 = Encoding.UTF8.GetString(string1State._originalString);
                    var str2 = Encoding.UTF8.GetString(string2State._originalString);
                }

                // else, it means they should be compared by string, again, we compare only the effective numbers
                // we compare the numbers as byte sequences, because both numbers are guaranteed to be of the same length
                return string1State._originalString.Slice((int)(string1State.CurrentBytePosition - string1State._numberLengthInBytes), (int)string1State._numberLengthInBytes)
                    .SequenceCompareTo(string2State._originalString.Slice((int)(string2State.CurrentBytePosition - string2State._numberLengthInBytes),
                        (int)string2State._numberLengthInBytes));
            }

            // if one of the sequences is a number and the other is not, the number is always smaller
            if (string1State._curSequenceIsNumber != string2State._curSequenceIsNumber)
            {
                if (string1State._curSequenceIsNumber)
                    return -1;
                return 1;
            }

            // should be case insensitive
            Span<char> ch1 = stackalloc char[4];
            Span<char> ch2 = stackalloc char[4];
            var offset1 = string1State._curSequenceStartPosition;
            var offset2 = string2State._curSequenceStartPosition;

            var length1 = string1State.CurrentBytePosition - string1State._curSequenceStartPosition;
            var length2 = string2State.CurrentBytePosition - string2State._curSequenceStartPosition;

            while (length1 > 0 && length2 > 0)
            {
                var (read1Bytes, read1Chars) = ReadCharacter(string1State._originalString, offset1, ch1);
                var (read2Bytes, read2Chars) = ReadCharacter(string2State._originalString, offset2, ch2);

                length1 -= read1Bytes;
                length2 -= read2Bytes;

                int result = read1Chars switch
                {
                    1 when read2Chars == 1 => char.ToLowerInvariant(ch1[0]) - char.ToLowerInvariant(ch2[0]),
                    2 when read2Chars == 2 => ch1.Slice(0, (int)read1Chars).SequenceCompareTo(ch2.Slice(0, (int)read2Chars)),
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

            return (int)(length1 - length2);
        }
    }
    
    public int Compare(ReadOnlySpan<byte> string1, ReadOnlySpan<byte> string2)
    {
        Span<char> buffers = stackalloc char[8];
        var string1State = new AlphanumericStringComparisonState(string1, buffers.Slice(0, 4));
        var string2State = new AlphanumericStringComparisonState(string2, buffers.Slice(4));

        
        // Walk through two the strings with two markers.
        while (string1State.CurrentBytePosition < string1State.StringLengthInBytes &&
               string2State.CurrentBytePosition < string2State.StringLengthInBytes)
        {
            string1State.ScanNextAlphabeticOrNumericSequence();
            string2State.ScanNextAlphabeticOrNumericSequence();

            var result = string1State.CompareWithAnotherState(ref string2State);
            if (result != 0)
            {
                return result;
            }
        }

        if (string1State.CurrentBytePosition < string1State.StringLengthInBytes)
            return 1;
        if (string2State.CurrentBytePosition < string2State.StringLengthInBytes)
            return -1;

        return 0;
    }
}
