using System;
using System.Text;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents
{
    internal unsafe class DocumentIdWorker
    {
        [ThreadStatic]
        private static JsonParserState _jsonParserState;

        public static ByteStringContext<ByteStringMemoryCache>.ExternalScope GetSliceFromId<TTransaction>(
            TransactionOperationContext<TTransaction> context, string id, out Slice idSlice)
            where TTransaction : RavenTransaction
        {
            var byteCount = Encoding.GetMaxByteCount(id.Length);

            var buffer = context.GetMemory(
                byteCount // this buffer is allocated to also serve the GetSliceFromUnicodeKey
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

            return Slice.External(context.Allocator, buffer.Address, (ushort)id.Length, out idSlice);

            UnlikelyUnicode:
            return GetSliceFromUnicodeKey(context, id, out idSlice, buffer.Address, byteCount);
        }

        private static ByteStringContext<ByteStringMemoryCache>.ExternalScope GetSliceFromUnicodeKey<TTransaction>(
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


        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetLower(ByteStringContext byteStringContext, LazyStringValue str, out Slice loweredKey)
        {
            return GetLower(byteStringContext, str.Buffer, str.Size, out loweredKey);
        }
        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetLower(ByteStringContext byteStringContext,  byte* str, int size, out Slice loweredKey)
        {
            var release = byteStringContext.Allocate(size, out var ptr);
            for (int i = 0; i < size; i++)
            {
                var ch = str[i];
                if ((ch < 65) || (ch > 90))
                {
                    if (ch > 127) // not ASCII, use slower mode
                        goto UnlikelyUnicode;

                    ptr.Ptr[i] = ch;
                }
                else
                {
                    ptr.Ptr[i] = (byte)(ch | 0x20);
                }
            }
            loweredKey = new Slice(ptr);
            return release;
            
            UnlikelyUnicode:
            release.Dispose();
            return UnlikelyGetLowerUnicode(byteStringContext, str, size, out loweredKey);
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope UnlikelyGetLowerUnicode(ByteStringContext byteStringContext, byte* str, int size, out Slice loweredKey)
        {
            var maxCharCount = Encoding.GetMaxCharCount(size);
            var bufferSize = maxCharCount* sizeof(char);
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


        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetLowerIdSliceAndStorageKey<TTransaction>(
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

            int strLength = str.Length;

            int idSize = JsonParserState.VariableSizeIntSize(strLength);

            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(str);

            var scope = context.Allocator.Allocate(strLength // lower key
                                       + idSize // the size of var int for the len of the key
                                       + strLength // actual key
                                       + escapePositionsSize, out ByteString buffer);


            byte* ptr = buffer.Ptr;
            fixed (char* pChars = str)
            {
                int size = Encoding.GetMaxByteCount(strLength);
                var strSize = Encoding.GetBytes(pChars, strLength, ptr, size);
                _jsonParserState.FindEscapePositionsIn(ptr, strSize, escapePositionsSize);
            }

            for (var i = 0; i < strLength; i++)
            {
                var ch = str[i];
                if ((ch < 65) || (ch > 90))
                {
                    if (ch > 127) // not ASCII, use slower mode
                        goto UnlikelyUnicode;

                    ptr[i] = (byte)ch;
                }
                else
                {
                    ptr[i] = (byte)(ch | 0x20);
                }


                ptr[i + idSize + strLength] = (byte)ch;
            }

            var writePos = ptr + strLength;

            JsonParserState.WriteVariableSizeInt(ref writePos, strLength);
            _jsonParserState.WriteEscapePositionsTo(writePos + strLength);
            idSize = escapePositionsSize + strLength + idSize;

            Slice.External(context.Allocator, ptr + strLength, idSize, out idSlice);
            Slice.External(context.Allocator, ptr, str.Length, out lowerIdSlice);
            return scope;

            UnlikelyUnicode:
            return UnicodeGetLowerIdAndStorageKey(context, str, out lowerIdSlice, out idSlice);
        }

        private static ByteStringContext.InternalScope UnicodeGetLowerIdAndStorageKey<TTransaction>(
            TransactionOperationContext<TTransaction> context, string str,
            out Slice lowerIdSlice, out Slice idSlice)
            where TTransaction : RavenTransaction
        {
            // See comment in GetLowerIdSliceAndStorageKey for the format
            _jsonParserState.Reset();
            var byteCount = Encoding.GetMaxByteCount(str.Length);
            var maxIdLenSize = JsonParserState.VariableSizeIntSize(byteCount);

            int strLength = str.Length;
            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(str);

            ByteString buffer;
            var scope = context.Allocator.Allocate(
                sizeof(char) * strLength // for the lower calls
                + byteCount // lower ID
                + maxIdLenSize // the size of var int for the len of the ID
                + byteCount // actual ID
                + escapePositionsSize, out buffer);

            fixed (char* pChars = str)
            {
                var size = Encoding.GetBytes(pChars, strLength, buffer.Ptr, byteCount);
                _jsonParserState.FindEscapePositionsIn(buffer.Ptr, size, escapePositionsSize);

                var destChars = (char*)buffer.Ptr;
                for (var i = 0; i < strLength; i++)
                    destChars[i] = char.ToLowerInvariant(pChars[i]);

                byte* lowerId = buffer.Ptr + strLength * sizeof(char);

                int lowerSize = Encoding.GetBytes(destChars, strLength, lowerId, byteCount);

                if (lowerSize > 512)
                    ThrowDocumentIdTooBig(str);

                byte* id = buffer.Ptr + strLength * sizeof(char) + byteCount;
                byte* writePos = id;
                int idSize = Encoding.GetBytes(pChars, strLength, writePos + maxIdLenSize, byteCount);

                var actualIdLenSize = JsonParserState.VariableSizeIntSize(idSize);
                if (actualIdLenSize < maxIdLenSize)
                {
                    var movePtr = maxIdLenSize - actualIdLenSize;
                    id += movePtr;
                    writePos += movePtr;
                }

                JsonParserState.WriteVariableSizeInt(ref writePos, idSize);
                _jsonParserState.WriteEscapePositionsTo(writePos + idSize);
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
            // TODO: Optimize this
            return GetLowerIdSliceAndStorageKey(context, str, out var _, out strSlice);
        }
    }
}