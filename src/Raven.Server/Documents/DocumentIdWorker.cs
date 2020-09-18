using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents
{
    internal unsafe class DocumentIdWorker
    {
        [ThreadStatic]
        private static JsonParserState _jsonParserState;

        public const int MaxIdSize = 512;

        static DocumentIdWorker()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => _jsonParserState = null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetSliceFromId<TTransaction>(
            TransactionOperationContext<TTransaction> context, string id, out Slice idSlice,
            byte? separator = null)
            where TTransaction : RavenTransaction
        {
            return GetSliceFromId(context, id.AsSpan(), out idSlice, separator);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetSliceFromId<TTransaction>(
            TransactionOperationContext<TTransaction> context, LazyStringValue id, out Slice idSlice,
            byte? separator = null)
            where TTransaction : RavenTransaction
        {
            var charCount = Encodings.Utf8.GetCharCount(id.Buffer, id.Size);
            var tempBuffer = ByteStringContext.ToLowerTempBuffer;
            if (tempBuffer == null || tempBuffer.Length < charCount)
                ByteStringContext.ToLowerTempBuffer = tempBuffer = new char[Bits.PowerOf2(charCount)];

            fixed (char* pChars = tempBuffer)
            {
                if(id.Size > 0)
                    charCount = Encodings.Utf8.GetChars(id.Buffer, id.Size, pChars, tempBuffer.Length);
                return GetSliceFromId(context, new Span<char>(pChars, charCount), out idSlice, separator);
            }
        }

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetSliceFromId<TTransaction>(TransactionOperationContext<TTransaction> context, ReadOnlySpan<char> id, out Slice idSlice, byte? separator = null)
            where TTransaction : RavenTransaction
        {
            if (_jsonParserState == null)
                _jsonParserState = new JsonParserState();

            _jsonParserState.Reset();

            var strLength = id.Length;

            var maxStrSize = Encoding.GetMaxByteCount(strLength);
            var escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(id, out _);

            if (strLength > MaxIdSize)
                ThrowDocumentIdTooBig(id.ToString());

            var internalScope = context.Allocator.Allocate(
                maxStrSize // this buffer is allocated to also serve the GetSliceFromUnicodeKey
                + sizeof(char) * id.Length
                + escapePositionsSize
                + (separator != null ? 1 : 0),
                out var buffer);

            idSlice = new Slice(buffer);

            for (var i = 0; i < id.Length; i++)
            {
                var ch = id[i];
                if (ch > 127) // not ASCII, use slower mode
                {
                    strLength = ReadFromUnicodeKey(id, buffer, maxStrSize, separator);
                    goto Finish;
                }

                if ((ch >= 65) && (ch <= 90))
                    buffer.Ptr[i] = (byte)(ch | 0x20);
                else
                    buffer.Ptr[i] = (byte)ch;
            }

            _jsonParserState.FindEscapePositionsIn(buffer.Ptr, ref strLength, escapePositionsSize);
            if (separator != null)
            {
                buffer.Ptr[strLength] = separator.Value;
                strLength++;
            }

            Finish:
            buffer.Truncate(strLength);
            return internalScope;
        }

        private static int ReadFromUnicodeKey(
            ReadOnlySpan<char> key,
            ByteString buffer,
            int maxByteCount,
            byte? separator)
        {
            var destChars = (char*)(buffer.Ptr + maxByteCount);
            for (var i = 0; i < key.Length; i++)
                destChars[i] = char.ToLowerInvariant(key[i]);
            var size = Encoding.GetBytes(destChars, key.Length, buffer.Ptr, maxByteCount);

            if (separator != null)
            {
                buffer.Ptr[size] = separator.Value;
                size++;
            }
            return size;
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
                    else if(ch > 127)
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

        public static ByteStringContext.InternalScope GetLowerIdSliceAndStorageKey<TTransaction>(
            TransactionOperationContext<TTransaction> context, string str, out Slice lowerIdSlice, out Slice idSlice)
            where TTransaction : RavenTransaction
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
            int strLength = originalStrLength;

            if (strLength > MaxIdSize)
                ThrowDocumentIdTooBig(str);

            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(str, out var escapedCount);

            /*
             *  add the size of all control characters
             *  this is to treat case when we have 2+ control character in a row
             *  GetMaxByteCount returns smaller size than the actual size with escaped control characters
             *  For example: string with two control characters such as '\0\0' will be converted to '\u0000\u0000' (another example: '\b\b' => '\u000b\u000b')
             *  string size = 2, GetMaxByteCount = 9, converted string size = 12, maxStrSize = 19
             */
            var maxStrSize = Encoding.GetMaxByteCount(strLength) + JsonParserState.ControlCharacterItemSize * escapedCount;
            var originalMaxStrSize = maxStrSize;

            int idSize = JsonParserState.VariableSizeIntSize(maxStrSize);

            var scope = context.Allocator.Allocate(maxStrSize // lower key
                                       + idSize // the size of var int for the len of the key
                                       + maxStrSize // actual key
                                       + escapePositionsSize, out ByteString buffer);


            byte* ptr = buffer.Ptr;

            fixed (char* pChars = str)
            {
                for (var i = 0; i < strLength; i++)
                {
                    uint ch = pChars[i];

                    // PERF: Trick to avoid multiple compare instructions on hot loops. 
                    //       This is the same as (ch >= 65 && ch <= 90)
                    if (ch - 65 <= 90 - 65)
                    {
                        ptr[i] = (byte)(ch | 0x20);
                    }
                    else
                    {
                        if (ch > 127) // not ASCII, use slower mode
                            goto UnlikelyUnicode;

                        ptr[i] = (byte)ch;
                    }

                    ptr[i + idSize + maxStrSize] = (byte)ch;
                }

                _jsonParserState.FindEscapePositionsIn(ptr, ref strLength, escapePositionsSize);
                if (strLength != originalStrLength)
                {
                    var anotherStrLength = originalStrLength;
                    _jsonParserState.FindEscapePositionsIn(ptr + idSize + maxStrSize, ref anotherStrLength, escapePositionsSize);

#if DEBUG
                    if (strLength != anotherStrLength)
                        throw new InvalidOperationException($"String length mismatch between Id ({str}) and it's lowercased counterpart after finding escape positions. Original: {anotherStrLength}. Lowercased: {strLength}");
#endif
                }
            }

            var writePos = ptr + maxStrSize;

            Debug.Assert(strLength <= originalMaxStrSize, $"Calculated {nameof(originalMaxStrSize)} value {originalMaxStrSize}, was smaller than actually {nameof(strLength)} value {strLength}");

            // in case there were no control characters the idSize could be smaller
            var sizeDifference = idSize - JsonParserState.VariableSizeIntSize(strLength);
            writePos += sizeDifference;
            idSize -= sizeDifference;

            JsonParserState.WriteVariableSizeInt(ref writePos, strLength);
            escapePositionsSize = _jsonParserState.WriteEscapePositionsTo(writePos + strLength);
            idSize = escapePositionsSize + strLength + idSize;

            Slice.External(context.Allocator, ptr + maxStrSize + sizeDifference, idSize, out idSlice);
            Slice.External(context.Allocator, ptr, strLength, out lowerIdSlice);
            return scope;

        UnlikelyUnicode:
            scope.Dispose();
            return UnicodeGetLowerIdAndStorageKey(context, str, out lowerIdSlice, out idSlice, maxStrSize, escapePositionsSize);
        }

        private static ByteStringContext.InternalScope UnicodeGetLowerIdAndStorageKey<TTransaction>(
            TransactionOperationContext<TTransaction> context, string str,
            out Slice lowerIdSlice, out Slice idSlice, int maxStrSize, int escapePositionsSize)
            where TTransaction : RavenTransaction
        {
            // See comment in GetLowerIdSliceAndStorageKey for the format

            var maxIdLenSize = JsonParserState.VariableSizeIntSize(maxStrSize);

            int strLength = str.Length;

            var scope = context.Allocator.Allocate(
                sizeof(char) * strLength // for the lower calls
                + maxStrSize // lower ID
                + maxIdLenSize // the size of var int for the len of the ID
                + maxStrSize // actual ID
                + escapePositionsSize, out ByteString buffer);

            fixed (char* pChars = str)
            {
                var size = Encoding.GetBytes(pChars, strLength, buffer.Ptr, maxStrSize);
                _jsonParserState.FindEscapePositionsIn(buffer.Ptr, ref size, escapePositionsSize);

                var destChars = (char*)buffer.Ptr;
                for (var i = 0; i < strLength; i++)
                    destChars[i] = char.ToLowerInvariant(pChars[i]);

                byte* lowerId = buffer.Ptr + strLength * sizeof(char);

                int lowerSize = Encoding.GetBytes(destChars, strLength, lowerId, maxStrSize);

                if (lowerSize > MaxIdSize)
                    ThrowDocumentIdTooBig(str);

                byte* id = buffer.Ptr + strLength * sizeof(char) + maxStrSize;
                byte* writePos = id;
                int idSize = Encoding.GetBytes(pChars, strLength, writePos + maxIdLenSize, maxStrSize);

                var actualIdLenSize = JsonParserState.VariableSizeIntSize(idSize);
                if (actualIdLenSize < maxIdLenSize)
                {
                    var movePtr = maxIdLenSize - actualIdLenSize;
                    id += movePtr;
                    writePos += movePtr;
                }

                JsonParserState.WriteVariableSizeInt(ref writePos, idSize);
                escapePositionsSize = _jsonParserState.WriteEscapePositionsTo(writePos + idSize);
                idSize += escapePositionsSize + maxIdLenSize;

                Slice.External(context.Allocator, id, idSize, out idSlice);
                Slice.External(context.Allocator, lowerId, lowerSize, out lowerIdSlice);
                return scope;
            }
        }

        public static void ThrowDocumentIdTooBig(string str)
        {
            throw new ArgumentException(
                $"Document ID cannot exceed {MaxIdSize} bytes, but the ID was {Encoding.GetByteCount(str)} bytes. The invalid ID is '{str}'.",
                nameof(str));
        }

        public static ByteStringContext.InternalScope GetStringPreserveCase(DocumentsOperationContext context, string str, out Slice strSlice)
        {
            return GetLowerIdSliceAndStorageKey(context, str, out var _, out strSlice);
        }
    }
}
