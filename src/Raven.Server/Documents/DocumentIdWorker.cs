using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents
{
    internal sealed unsafe class DocumentIdWorker
    {
        [ThreadStatic]
        private static JsonParserState _jsonParserState;

        public const int MaxIdSize = 512;
        public const uint MaxAsciiCodePoint = 127;
        public const int RevisionMaxKeySize = MaxIdSize * 3;

        static DocumentIdWorker()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => _jsonParserState = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetLoweredIdSliceFromId<TTransaction>(
            TransactionOperationContext<TTransaction> context, LazyStringValue id, out Slice lowerIdSlice,
            byte? separator = null)
            where TTransaction : RavenTransaction
        {
            var charCount = Encodings.Utf8.GetCharCount(id.Buffer, id.Size);
            var tempBuffer = ByteStringContext.GetThreadStaticBufferOf(charCount);

            fixed (char* pChars = tempBuffer)
            {
                if (id.Size > 0)
                    charCount = Encodings.Utf8.GetChars(id.Buffer, id.Size, pChars, tempBuffer.Length);
                return GetLoweredIdSliceFromId(context.Allocator, new Span<char>(pChars, charCount), out lowerIdSlice, separator);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetLoweredIdSliceFromId<TTransaction>(TransactionOperationContext<TTransaction> context,
            ReadOnlySpan<char> id, out Slice lowerIdSlice, byte? separator = null)
            where TTransaction : RavenTransaction
        {
            return GetLoweredIdSliceFromId(context.Allocator, id, out lowerIdSlice, separator);
        }

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetLoweredIdSliceFromId(ByteStringContext allocator, string id, out Slice lowerIdSlice, byte? separator = null)
        {
            return GetLoweredIdSliceFromId(allocator, id.AsSpan(), out lowerIdSlice, separator);
        }
        
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetLoweredIdSliceFromId(ByteStringContext allocator, ReadOnlySpan<char> id, out Slice lowerIdSlice, byte? separator = null)
        {
            if (_jsonParserState == null)
                _jsonParserState = new JsonParserState();

            _jsonParserState.Reset();

            var strLength = id.Length;

            var maxStrSize = Encoding.GetMaxByteCount(strLength);
            var escapeAndControlSize = StringUtils.FindMaxEscapePositionAndControlCharSizeForBackwardCompatibility(id, out _);

            if (strLength > MaxIdSize)
                ThrowDocumentIdTooBig(id);

            var internalScope = allocator.Allocate(
                maxStrSize // this buffer is allocated to also serve the ReadFromUnicodeKey
                + sizeof(char) * id.Length
                + escapeAndControlSize
                + (separator != null ? 1 : 0),
                out var buffer);

            lowerIdSlice = new Slice(buffer);

            for (var i = 0; i < id.Length; i++)
            {
                var ch = id[i];
                if (ch > 127) // not ASCII, use slower mode
                {
                    strLength = ReadFromUnicodeKey(id, buffer, maxStrSize);
                    break;
                }

                if ((ch >= 65) && (ch <= 90))
                    buffer.Ptr[i] = (byte)(ch | 0x20);
                else
                    buffer.Ptr[i] = (byte)ch;
            }

            _jsonParserState.FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(buffer.Ptr, ref strLength, escapeAndControlSize);
            if (separator != null)
            {
                buffer.Ptr[strLength] = separator.Value;
                strLength++;
            }

            buffer.Truncate(strLength);
            return internalScope;
        }

        private static int ReadFromUnicodeKey(ReadOnlySpan<char> key, ByteString buffer, int maxByteCount)
        {
            var destChars = (char*)(buffer.Ptr + maxByteCount);
            for (var i = 0; i < key.Length; i++)
                destChars[i] = char.ToLowerInvariant(key[i]);
            return Encoding.GetBytes(destChars, key.Length, buffer.Ptr, maxByteCount);
        }

        
        private static readonly UTF8Encoding Encoding = new UTF8Encoding();

        public static ByteStringContext.InternalScope GetLower(ByteStringContext byteStringContext, LazyStringValue str, out Slice loweredKey)
        {
            return GetLower(byteStringContext, str.Buffer, str.Size, out loweredKey);
        }

        public static ByteStringContext.InternalScope GetLower(ByteStringContext byteStringContext, string str, out Slice loweredKey)
        {
            fixed (char* pCh = str)
            {
                var release = byteStringContext.Allocate(str.Length, out var ptr);

                byte* pointer = ptr.Ptr;
                for (int i = 0; i < str.Length; i++)
                {
                    uint ch = pCh[i];

                    if (ch >= 65) // 65 = 'A'
                    {
                        if (ch <= 90) // 90 = 'Z'
                            ch = (byte)(ch | 0x20); //Turn on the sixth bit to apply lower case 
                        else if (ch > 127)
                            goto UnlikelyUnicode; // not ASCII, use slower mode
                    }

                    pointer[i] = (byte)ch;
                }
                loweredKey = new Slice(ptr);
                return release;

                UnlikelyUnicode:
                release.Dispose();

                return UnlikelyGetLowerUnicode(byteStringContext, str, out loweredKey);
            }
        }

        public static ByteStringContext.InternalScope GetLower(ByteStringContext byteStringContext, byte* str, int size, out Slice loweredKey)
        {
            var release = byteStringContext.Allocate(size, out var ptr);

            byte* pointer = ptr.Ptr;
            for (int i = 0; i < size; i++)
            {
                byte ch = str[i];

                if (ch >= 65) // 65 = 'A'
                {
                    if (ch <= 90) // 90 = 'Z'
                        ch = (byte)(ch | 0x20); //Turn on the sixth bit to apply lower case 
                    else if (ch > 127)
                        goto UnlikelyUnicode; // not ASCII, use slower mode
                }

                pointer[i] = ch;
            }
            loweredKey = new Slice(ptr);
            return release;

        UnlikelyUnicode:
            release.Dispose();
            return UnlikelyGetLowerUnicode(byteStringContext, str, size, out loweredKey);
        }

        private static ByteStringContext.InternalScope UnlikelyGetLowerUnicode(ByteStringContext byteStringContext, byte* str, int size, out Slice loweredKey)
        {
            var maxCharCount = Encoding.GetMaxCharCount(size);
            var bufferSize = maxCharCount * sizeof(char);
            using (byteStringContext.Allocate(bufferSize, out var ptr))
            {
                var chars = (char*)ptr.Ptr;
                var charCount = Encoding.GetChars(str, size, chars, maxCharCount);

                for (int i = 0; i < charCount; i++)
                {
                    chars[i] = char.ToLowerInvariant(chars[i]);
                }

                var release = byteStringContext.From(chars, charCount, ByteStringType.Immutable, out var result);
                loweredKey = new Slice(result);
                return release;
            }

        }

        private static ByteStringContext.InternalScope UnlikelyGetLowerUnicode(ByteStringContext byteStringContext, string str, out Slice loweredKey)
        {
            var maxCharCount = Encoding.GetMaxCharCount(str.Length);
            var bufferSize = maxCharCount * sizeof(char);

            fixed (char* pCh = str)
            {
                using (byteStringContext.Allocate(bufferSize, out var ptr))
                {
                    var chars = (char*)ptr.Ptr;

                    for (int i = 0; i < str.Length; i++)
                    {
                        chars[i] = char.ToLowerInvariant(pCh[i]);
                    }

                    var release = byteStringContext.From(chars, str.Length, ByteStringType.Immutable, out var result);
                    loweredKey = new Slice(result);
                    return release;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        // RavenDB-25738 Control character are invalid for new databases
        // but use it for backward compatibility for document ID, collection name, attachment name, attachment content, and time series tag
        public static ByteStringContext.InternalScope GetLowerIdSliceAndStorageKeyForBackwardCompatibility<TTransaction>(
            TransactionOperationContext<TTransaction> context, string str, out Slice lowerIdSlice, out Slice idSlice)
            where TTransaction : RavenTransaction
        {
            return GetLowerIdSliceAndStorageKeyForBackwardCompatibility(context.Allocator, str, out lowerIdSlice, out idSlice);
        }

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetLowerIdSliceAndStorageKeyForBackwardCompatibility(ByteStringContext allocator, ReadOnlySpan<char> str, out Slice lowerIdSlice,
            out Slice idSlice)
        {
            // Because we need to also store escape positions for the key when we store it
            // we need to store it as a lazy string value.
            // But lazy string value has two lengths, one is the string length, and the other 
            // is the actual data size with the escape positions

            // In order to resolve this, we process the key to find escape positions, then store it 
            // in the table using the following format:
            //
            // [var int - string len, string bytes, number of escape positions, escape positions]
            //
            // The total length of the string is stored in the actual table (and include the var int size 
            // prefix.

            if (_jsonParserState == null)
                _jsonParserState = new JsonParserState();

            _jsonParserState.Reset();

            int originalStrLength = str.Length;
            if (originalStrLength > MaxIdSize)
                ThrowDocumentIdTooBig(str);

            int escapePositionsSize = StringUtils.FindMaxEscapePositionAndControlCharSizeForBackwardCompatibility(str, out var controlCount);

            /*
             *  add the size of all control characters
             *  this is to treat case when we have 2+ control character in a row
             *  GetMaxByteCount returns smaller size than the actual size with escaped control characters
             *  For example: string with two control characters such as '\0\0' will be converted to '\u0000\u0000' (another example: '\b\b' => '\u000b\u000b')
             *  string size = 2, GetMaxByteCount = 9, converted string size = 12, maxStrSize = 19
             */
            var maxIdSize = Encoding.GetMaxByteCount(originalStrLength) + JsonParserState.ControlCharacterItemSize * controlCount;
            var originalMaxStrSize = maxIdSize;

            int maxIdLenSize = JsonParserState.VariableSizeIntSize(maxIdSize);

            var scope = allocator.Allocate(maxIdSize // lower key
                                       + maxIdLenSize // the size of var int for the len of the key
                                       + maxIdSize // actual key
                                       + escapePositionsSize, out ByteString buffer);
            
            byte* ptr = buffer.Ptr;

            for (var i = 0; i < str.Length; i++)
            {
                uint ch = str[i];

                // PERF: Trick to avoid multiple compare instructions on hot loops. 
                //       This is the same as (ch >= 65 && ch <= 90)
                if (ch - 65 <= 90 - 65)
                {
                    ptr[i] = (byte)(ch | 0x20);
                }
                else
                {
                    if (ch > MaxAsciiCodePoint) // not ASCII, use slower mode
                        goto UnlikelyUnicode;

                    ptr[i] = (byte)ch;
                }

                ptr[i + maxIdLenSize + maxIdSize] = (byte)ch;
            }

            int lowerIdLength = originalStrLength;
            _jsonParserState.FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(ptr, ref lowerIdLength, escapePositionsSize);
            if (lowerIdLength != originalStrLength)
            {
                var idLength = originalStrLength;
                _jsonParserState.FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(ptr + maxIdLenSize + maxIdSize, ref idLength, escapePositionsSize);

#if DEBUG
                if (lowerIdLength != idLength)
                    throw new InvalidOperationException($"String length mismatch between Id ({str}) and it's lowercased counterpart after finding escape positions. Original: {idLength}. Lowercased: {lowerIdLength}");
#endif
            }

            var writePos = ptr + maxIdSize;

            Debug.Assert(lowerIdLength <= originalMaxStrSize, $"Calculated {nameof(originalMaxStrSize)} value {originalMaxStrSize}, was smaller than actually {nameof(lowerIdLength)} value {lowerIdLength}");

            // in case there were no control characters the idSize could be smaller
            var sizeDifference = maxIdLenSize - JsonParserState.VariableSizeIntSize(lowerIdLength);
            writePos += sizeDifference;
            maxIdLenSize -= sizeDifference;

            JsonParserState.WriteVariableSizeInt(ref writePos, lowerIdLength);
            escapePositionsSize = _jsonParserState.WriteEscapePositionsTo(writePos + lowerIdLength);
            maxIdLenSize = escapePositionsSize + lowerIdLength + maxIdLenSize;

            Slice.External(allocator, ptr + maxIdSize + sizeDifference, maxIdLenSize, out idSlice);
            Slice.External(allocator, ptr, lowerIdLength, out lowerIdSlice);

            Debug.Assert(ptr + maxIdSize + sizeDifference + maxIdLenSize <= buffer.Ptr + buffer.Size, "Exceed buffer size");
            return scope;

        UnlikelyUnicode:
            scope.Dispose();
            return UnicodeGetLowerIdAndStorageKey(allocator, str, out lowerIdSlice, out idSlice, maxIdSize, maxIdLenSize, escapePositionsSize);
        }

        private static ByteStringContext.InternalScope UnicodeGetLowerIdAndStorageKey(
            ByteStringContext allocator, ReadOnlySpan<char> str,
            out Slice lowerIdSlice, out Slice idSlice, int maxStrSize, int maxIdLenSize, int escapePositionsSize)
        {
            // See comment in GetLowerIdSliceAndStorageKeyForBackwardCompatibility for the format

            int strLength = str.Length;

            var scope = allocator.Allocate(
                sizeof(char) * strLength // for the lower calls
                + maxStrSize // lower ID
                + maxIdLenSize // the size of var int for the len of the ID
                + maxStrSize // actual ID
                + escapePositionsSize, out ByteString buffer);

            fixed (char* pChars = str)
            {
                var destChars = (char*)buffer.Ptr;
                for (var i = 0; i < strLength; i++)
                    destChars[i] = char.ToLowerInvariant(str[i]);

                byte* lowerId = buffer.Ptr + strLength * sizeof(char);

                int lowerIdSize = Encoding.GetBytes(destChars, strLength, lowerId, maxStrSize);
                if (lowerIdSize > MaxIdSize)
                    ThrowDocumentIdTooBig(str);
                
                var originalLowerSize = lowerIdSize;
                _jsonParserState.FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(lowerId, ref lowerIdSize, escapePositionsSize);
                
                byte* actualIdPtr = buffer.Ptr + strLength * sizeof(char) + maxStrSize;
                int actualIdSize = Encoding.GetBytes(pChars, strLength, actualIdPtr + maxIdLenSize, maxStrSize);
                
                var actualIdLenSize = JsonParserState.VariableSizeIntSize(actualIdSize);
                if (actualIdLenSize < maxIdLenSize)
                    actualIdPtr += maxIdLenSize - actualIdLenSize;

                byte* writePos = actualIdPtr;
                
                //We already checked if there are control characters to escape
                if (originalLowerSize != lowerIdSize)
                    _jsonParserState.FindEscapedPositionsAndEscapeControlsForBackwardCompatibility(actualIdPtr + maxIdLenSize, ref actualIdSize, escapePositionsSize);

                JsonParserState.WriteVariableSizeInt(ref writePos, actualIdSize);
                escapePositionsSize = _jsonParserState.WriteEscapePositionsTo(writePos + actualIdSize);
                actualIdSize += escapePositionsSize + actualIdLenSize;

                Slice.External(allocator, actualIdPtr, actualIdSize, out idSlice);
                Slice.External(allocator, lowerId, lowerIdSize, out lowerIdSlice);
                
                Debug.Assert(actualIdPtr + actualIdSize <= buffer.Ptr + buffer.Size, "Exceed buffer size");
                return scope;
            }
        }

        [DoesNotReturn]
        public static void ThrowDocumentIdTooBig(ReadOnlySpan<char> str)
        {
            throw new ArgumentException(
                $"Document ID cannot exceed {MaxIdSize} bytes, but the ID was {Encoding.GetByteCount(str)} bytes. The invalid ID is '{str}'.",
                nameof(str));
        }

        public static void ThrowRevisionKeyTooBig(string id, string changeVector, bool isTombstone)
        {
            var type = isTombstone ? "Revision Tombstone" : "Revision";
            throw new ArgumentException(
                $"{type} change vector cannot exceed {RevisionMaxKeySize} bytes, but the change vector was {Encoding.GetByteCount(changeVector)} bytes. " +
                $"The invalid change vector for {type} '{id}' is '{changeVector}'.{Environment.NewLine}" +
                $"For more details visit https://ravendb.net/l/28JF7X/7.1. {Environment.NewLine}", nameof(changeVector));
        }

        public static ByteStringContext.InternalScope GetStringPreserveCase(DocumentsOperationContext context, string str, out Slice strSlice)
        {
            return GetLowerIdSliceAndStorageKeyForBackwardCompatibility(context, str, out var _, out strSlice);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CheckAndThrowContainsControlCharacters(ReadOnlySpan<char> str, string identifierName)
        {
            if (StringUtils.HasControlCharacters(str))
                ThrowIdentifierWithControlCharacters(str, identifierName);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void CheckAndThrowContainsControlCharacters(LazyStringValue str, string identifierName)
        {
            if (str == null)
                return;

            if (StringUtils.HasControlCharacters(str.AsReadOnlySpan()))
                ThrowIdentifierWithControlCharacters(str.ToString().AsSpan(), identifierName);
        }

        [DoesNotReturn]
        public static void ThrowIdentifierWithControlCharacters(ReadOnlySpan<char> str, string identifierName)
        {
            throw new NotSupportedException($"{identifierName} cannot contain control characters: '{EscapeControlCharacters(str)}' (escaped version)");
            
            static string EscapeControlCharacters(ReadOnlySpan<char> str)
            {
                var sb = new StringBuilder();
                foreach (var c in str)
                {
                    if (StringUtils.IsControlCharacter(c) == false)
                    {
                        sb.Append(c);
                        continue;
                    }
                    sb.Append("\\u");
                    sb.Append(((int)c).ToString("x4"));
                }
                return sb.ToString();
            }
        }
    }
}
