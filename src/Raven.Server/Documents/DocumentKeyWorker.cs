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
    internal unsafe class DocumentKeyWorker
    {
        [ThreadStatic]
        private static JsonParserState _jsonParserState;

        public static ByteStringContext<ByteStringMemoryCache>.ExternalScope GetSliceFromKey<TTransaction>(
            TransactionOperationContext<TTransaction> context, string key, out Slice keySlice)
            where TTransaction : RavenTransaction
        {
            var byteCount = Encoding.GetMaxByteCount(key.Length);

            var buffer = context.GetMemory(
                byteCount // this buffer is allocated to also serve the GetSliceFromUnicodeKey
                + sizeof(char) * key.Length);


            if (key.Length > 512)
                ThrowKeyTooBig(key);


            for (var i = 0; i < key.Length; i++)
            {
                var ch = key[i];
                if (ch > 127) // not ASCII, use slower mode
                    goto UnlikelyUnicode;
                if ((ch >= 65) && (ch <= 90))
                    buffer.Address[i] = (byte)(ch | 0x20);
                else
                    buffer.Address[i] = (byte)ch;
            }

            return Slice.External(context.Allocator, buffer.Address, (ushort)key.Length, out keySlice);

            UnlikelyUnicode:
            return GetSliceFromUnicodeKey(context, key, out keySlice, buffer.Address, byteCount);
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
                    ThrowKeyTooBig(key);

                return Slice.External(context.Allocator, keyBytes, (ushort)size, out keySlice);
            }
        }


        private static readonly UTF8Encoding Encoding = new UTF8Encoding();

        public static ByteStringContext<ByteStringMemoryCache>.InternalScope GetLowerKeySliceAndStorageKey<TTransaction>(TransactionOperationContext<TTransaction> context, string str, out Slice lowerKeySlice, out Slice keySlice)
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

            int keySize = JsonParserState.VariableSizeIntSize(strLength);

            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(str);

            ByteString buffer;
            var scope = context.Allocator.Allocate(strLength // lower key
                                                   + keySize // the size of var int for the len of the key
                                                   + strLength // actual key
                                                   + escapePositionsSize, out buffer);


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


                ptr[i + keySize + strLength] = (byte)ch;
            }

            var writePos = ptr + strLength;

            JsonParserState.WriteVariableSizeInt(ref writePos, strLength);
            _jsonParserState.WriteEscapePositionsTo(writePos + strLength);
            keySize = escapePositionsSize + strLength + keySize;

            Slice.External(context.Allocator, ptr + strLength, keySize, out keySlice);
            Slice.External(context.Allocator, ptr, str.Length, out lowerKeySlice);
            return scope;

            UnlikelyUnicode:
            return UnicodeGetLowerKeySliceAndStorageKey(context, str, out lowerKeySlice, out keySlice);
        }

        private static ByteStringContext.InternalScope UnicodeGetLowerKeySliceAndStorageKey<TTransaction>(TransactionOperationContext<TTransaction> context, string str,
            out Slice lowerKeySlice, out Slice keySlice)
            where TTransaction : RavenTransaction
        {
            // See comment in GetLowerKeySliceAndStorageKey for the format
            _jsonParserState.Reset();
            var byteCount = Encoding.GetMaxByteCount(str.Length);
            var maxKeyLenSize = JsonParserState.VariableSizeIntSize(byteCount);

            int strLength = str.Length;
            int escapePositionsSize = JsonParserState.FindEscapePositionsMaxSize(str);

            ByteString buffer;
            var scope = context.Allocator.Allocate(
                sizeof(char) * strLength // for the lower calls
                + byteCount // lower key
                + maxKeyLenSize // the size of var int for the len of the key
                + byteCount // actual key
                + escapePositionsSize, out buffer);

            fixed (char* pChars = str)
            {
                var size = Encoding.GetBytes(pChars, strLength, buffer.Ptr, byteCount);
                _jsonParserState.FindEscapePositionsIn(buffer.Ptr, size, escapePositionsSize);

                var destChars = (char*)buffer.Ptr;
                for (var i = 0; i < strLength; i++)
                    destChars[i] = char.ToLowerInvariant(pChars[i]);

                byte* lowerKey = buffer.Ptr + strLength * sizeof(char);

                int lowerSize = Encoding.GetBytes(destChars, strLength, lowerKey, byteCount);

                if (lowerSize > 512)
                    ThrowKeyTooBig(str);

                byte* key = buffer.Ptr + strLength * sizeof(char) + byteCount;
                byte* writePos = key;
                int keySize = Encoding.GetBytes(pChars, strLength, writePos + maxKeyLenSize, byteCount);

                var actualKeyLenSize = JsonParserState.VariableSizeIntSize(keySize);
                if (actualKeyLenSize < maxKeyLenSize)
                {
                    var movePtr = maxKeyLenSize - actualKeyLenSize;
                    key += movePtr;
                    writePos += movePtr;
                }

                JsonParserState.WriteVariableSizeInt(ref writePos, keySize);
                _jsonParserState.WriteEscapePositionsTo(writePos + keySize);
                keySize += escapePositionsSize + maxKeyLenSize;

                Slice.External(context.Allocator, key, keySize, out keySlice);
                Slice.External(context.Allocator, lowerKey, lowerSize, out lowerKeySlice);
                return scope;
            }
        }

        private static void ThrowKeyTooBig(string str)
        {
            throw new ArgumentException(
                $"Key cannot exceed 512 bytes, but the key was {Encoding.GetByteCount(str)} bytes. The invalid key is '{str}'.",
                nameof(str));
        }

        public static ByteStringContext.InternalScope GetStringPreserveCase(DocumentsOperationContext context, string str, out Slice strSlice)
        {
            // TODO: Optimize this
            return GetLowerKeySliceAndStorageKey(context, str, out var _, out strSlice);
        }
    }
}