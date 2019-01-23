using System;
using System.Text;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents
{
    internal unsafe class DocumentIdWorker
    {
        [ThreadStatic]
        private static JsonParserState _jsonParserState;

        static DocumentIdWorker()
        {
            ThreadLocalCleanup.ReleaseThreadLocalState += () => _jsonParserState = null;
        }

        public static ByteStringContext.ExternalScope GetSliceFromId<TTransaction>(
            TransactionOperationContext<TTransaction> context, string id, out Slice idSlice)
            where TTransaction : RavenTransaction
        {
            if (_jsonParserState == null)
                _jsonParserState = new JsonParserState();

            _jsonParserState.Reset();

            var strLength = id.Length;
            var maxStrSize = Encoding.GetMaxByteCount(strLength);
            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(id);

            var buffer = context.GetMemory(
                maxStrSize  // this buffer is allocated to also serve the GetSliceFromUnicodeKey
                + sizeof(char) * id.Length);

            if (id.Length > 512)
                ThrowDocumentIdTooBig(id);

            for (var i = 0; i < id.Length; i++)
            {
                var ch = id[i];
                if (ch > 127) // not ASCII, use slower mode
                    goto UnlikelyUnicode;
                if ((ch >= 65) && (ch <= 90))
                    buffer.Address[i] = (byte)(ch | 0x20);
                else
                    buffer.Address[i] = (byte)ch;
            }

            _jsonParserState.FindEscapePositionsIn(buffer.Address, ref strLength, escapePositionsSize);

            return Slice.External(context.Allocator, buffer.Address, strLength, out idSlice);

        UnlikelyUnicode:
            return GetSliceFromUnicodeKey(context, id, out idSlice, buffer.Address, maxStrSize);
        }

        private static ByteStringContext.ExternalScope GetSliceFromUnicodeKey<TTransaction>(
            TransactionOperationContext<TTransaction> context,
            string key,
            out Slice keySlice,
            byte* buffer, int byteCount)
            where TTransaction : RavenTransaction
        {
            fixed (char* pChars = key)
            {
                var destChars = (char*)buffer;
                for (var i = 0; i < key.Length; i++)
                    destChars[i] = char.ToLowerInvariant(pChars[i]);

                var keyBytes = buffer + key.Length * sizeof(char);

                var size = Encoding.GetBytes(destChars, key.Length, keyBytes, byteCount);

                if (size > 512)
                    ThrowDocumentIdTooBig(key);

                return Slice.External(context.Allocator, keyBytes, (ushort)size, out keySlice);
            }
        }


        private static readonly UTF8Encoding Encoding = new UTF8Encoding();


        public static ByteStringContext.InternalScope GetLower(ByteStringContext byteStringContext, LazyStringValue str, out Slice loweredKey)
        {
            return GetLower(byteStringContext, str.Buffer, str.Size, out loweredKey);
        }
        public static ByteStringContext.InternalScope GetLower(ByteStringContext byteStringContext, byte* str, int size, out Slice loweredKey)
        {
            var release = byteStringContext.Allocate(size, out var ptr);

            byte* pointer = ptr.Ptr;
            for (int i = 0; i < size; i++)
            {
                byte ch = str[i];

                // PERF: Trick to avoid multiple compare instructions on hot loops. 
                //       This is the same as (ch >= 65 && ch <= 90)
                if (ch - 65 <= 90 - 65)
                {
                    ch = (byte)(ch | 0x20);
                }
                else
                {
                    if (ch > 127) // not ASCII, use slower mode
                        goto UnlikelyUnicode;
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
            int maxStrSize = Encoding.GetMaxByteCount(strLength);

            int idSize = JsonParserState.VariableSizeIntSize(strLength);

            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(str);

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

            JsonParserState.WriteVariableSizeInt(ref writePos, strLength);
            escapePositionsSize = _jsonParserState.WriteEscapePositionsTo(writePos + strLength);
            idSize = escapePositionsSize + strLength + idSize;

            Slice.External(context.Allocator, ptr + maxStrSize, idSize, out idSlice);
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

                if (lowerSize > 512)
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

        private static void ThrowDocumentIdTooBig(string str)
        {
            throw new ArgumentException(
                $"Document ID cannot exceed 512 bytes, but the ID was {Encoding.GetByteCount(str)} bytes. The invalid ID is '{str}'.",
                nameof(str));
        }

        public static ByteStringContext.InternalScope GetStringPreserveCase(DocumentsOperationContext context, string str, out Slice strSlice)
        {
            return GetLowerIdSliceAndStorageKey(context, str, out var _, out strSlice);
        }
    }
}
