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
        private static readonly Encoding Utf8 = Encoding.UTF8;

        [ThreadStatic]
        private static JsonParserState _jsonParserState;


        public static ByteStringContext<ByteStringMemoryCache>.ExternalScope GetSliceFromKey<TTransaction>(
            TransactionOperationContext<TTransaction> context, string key, out Slice keySlice)
            where TTransaction : RavenTransaction
        {
            var byteCount = Utf8.GetMaxByteCount(key.Length);

            var buffer = context.GetNativeTempBuffer(
                byteCount // this buffer is allocated to also serve the GetSliceFromUnicodeKey
                + sizeof(char)*key.Length);


            if (key.Length > 512)
                ThrowKeyTooBig(key);


            for (var i = 0; i < key.Length; i++)
            {
                var ch = key[i];
                if (ch > 127) // not ASCII, use slower mode
                    goto UnlikelyUnicode;
                if ((ch >= 65) && (ch <= 90))
                    buffer[i] = (byte) (ch | 0x20);
                else
                    buffer[i] = (byte) ch;
            }

            return Slice.External(context.Allocator, buffer, (ushort) key.Length, out keySlice);

            UnlikelyUnicode:
            return GetSliceFromUnicodeKey(context, key, out keySlice, buffer, byteCount);
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
                var destChars = (char*) buffer;
                for (var i = 0; i < key.Length; i++)
                    destChars[i] = char.ToLowerInvariant(pChars[i]);

                var keyBytes = buffer + key.Length*sizeof(char);

                var size = Utf8.GetBytes(destChars, key.Length, keyBytes, byteCount);

                if (size > 512)
                    ThrowKeyTooBig(key);

                return Slice.External(context.Allocator, keyBytes, (ushort) size, out keySlice);
            }
        }


        public static void GetLowerKeySliceAndStorageKey(JsonOperationContext context, string str, out byte* lowerKey,
            out int lowerSize,
            out byte* key, 
            out int keySize)
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

            keySize = JsonParserState.VariableSizeIntSize(str.Length);
            _jsonParserState.FindEscapePositionsIn(str);
            var escapePositionsSize = _jsonParserState.GetEscapePositionsSize();
            var buffer = context.GetNativeTempBuffer(
                str.Length // lower key
                + keySize // the size of var int for the len of the key
                + str.Length // actual key
                + escapePositionsSize);
            for (var i = 0; i < str.Length; i++)
            {
                var ch = str[i];
                if (ch > 127) // not ASCII, use slower mode
                    goto UnlikelyUnicode;
                if ((ch >= 65) && (ch <= 90))
                    buffer[i] = (byte) (ch | 0x20);
                else
                    buffer[i] = (byte) ch;

                buffer[i + keySize + str.Length] = (byte) ch;
            }

            var writePos = buffer + str.Length;

            JsonParserState.WriteVariableSizeInt(ref writePos, str.Length);
            _jsonParserState.WriteEscapePositionsTo(writePos + str.Length);
            keySize = escapePositionsSize + str.Length + keySize;
            key = buffer + str.Length;
            lowerKey = buffer;
            lowerSize = str.Length;
            return;


            UnlikelyUnicode:
            UnicodeGetLowerKeySliceAndStorageKey(context, str, out lowerKey, out lowerSize, out key, out keySize);
        }

        private static void UnicodeGetLowerKeySliceAndStorageKey(JsonOperationContext context, string str,
            out byte* lowerKey, out int lowerSize,
            out byte* key, out int keySize)
        {
            // See comment in GetLowerKeySliceAndStorageKey for the format
            _jsonParserState.Reset();
            var byteCount = Utf8.GetMaxByteCount(str.Length);
            _jsonParserState.FindEscapePositionsIn(str);
            var maxKeyLenSize = JsonParserState.VariableSizeIntSize(byteCount);
            var escapePositionsSize = _jsonParserState.GetEscapePositionsSize();
            var buffer = context.GetNativeTempBuffer(
                sizeof(char)*str.Length // for the lower calls
                + byteCount // lower key
                + maxKeyLenSize // the size of var int for the len of the key
                + byteCount // actual key
                + escapePositionsSize);

            fixed (char* pChars = str)
            {
                var destChars = (char*) buffer;
                for (var i = 0; i < str.Length; i++)
                    destChars[i] = char.ToLowerInvariant(pChars[i]);

                lowerKey = buffer + str.Length*sizeof(char);

                lowerSize = Utf8.GetBytes(destChars, str.Length, lowerKey, byteCount);

                if (lowerSize > 512)
                    ThrowKeyTooBig(str);

                key = buffer + str.Length*sizeof(char) + byteCount;
                var writePos = key;
                keySize = Utf8.GetBytes(pChars, str.Length, writePos + maxKeyLenSize, byteCount);

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
            }
        }

        private static void ThrowKeyTooBig(string str)
        {
            throw new ArgumentException(
                $"Key cannot exceed 512 bytes, but the key was {Utf8.GetByteCount(str)} bytes. The invalid key is '{str}'.",
                nameof(str));
        }
    }
}