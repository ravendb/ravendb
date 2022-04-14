using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.ContinuationTokens
{
    public abstract class ContinuationToken : IDynamicJson
    {
        public const string ContinuationTokenQueryString = "continuationToken";
        public const string PropertyName = "ContinuationToken";
        private static Dictionary<Type, Func<BlittableJsonReaderObject, ContinuationToken>> ConverterCache =
            new Dictionary<Type, Func<BlittableJsonReaderObject, ContinuationToken>>();

        public string ToBase64(JsonOperationContext context)
        {
            var json = context.ReadObject(ToJson(), $"token for {GetType()}");
            return Convert.ToBase64String(json.AsSpan());
        }

        public abstract DynamicJsonValue ToJson();

        public static T FromBase64<T>(JsonOperationContext context, string token) where T : ContinuationToken
        {
            if (token == null)
                return default;

            using (var json = Base64ToBlittable(context, token))
            {
                if (ConverterCache.TryGetValue(typeof(T), out var converter))
                    return (T)converter(json);

                converter = JsonDeserializationBase.GenerateJsonDeserializationRoutine<T>();
                var local = new Dictionary<Type, Func<BlittableJsonReaderObject, ContinuationToken>>(ConverterCache);
                if (local.TryAdd(typeof(T), converter))
                    Interlocked.Exchange(ref ConverterCache, local);

                return (T)converter(json);
            }
        }

        private static unsafe BlittableJsonReaderObject Base64ToBlittable(JsonOperationContext context, string json)
        {

            if (TryGetStackSafeBytesLength(json, out var length))
            {
                Span<byte> stackBuffer = stackalloc byte[length];
                return Base64ToBlittable(context, json, stackBuffer);
            }

            var rent = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                var heapBuffer = new Span<byte>(rent, 0, length);
                return Base64ToBlittable(context, json, heapBuffer);

            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rent);
            }
        }

        private static unsafe BlittableJsonReaderObject Base64ToBlittable(JsonOperationContext context, string json, Span<byte> bytes)
        {
            if (Convert.TryFromBase64String(json, bytes, out var length) == false)
                throw new InvalidOperationException("Invalid Base64 format");

            fixed (byte* ptr = bytes)
            {
                using (var blittable = new BlittableJsonReaderObject(ptr, length, context))
                {
                    blittable.BlittableValidation(); //precaution, needed because this is user input..
                    return blittable.Clone(context); // we clone in order to move the memory ownership to the context
                }
            }
        }

        private static bool TryGetStackSafeBytesLength(string str, out int length)
        {
            length = Encoding.UTF8.GetMaxByteCount(str.Length);
            if (length < 256)
                return true;

            length = Encoding.UTF8.GetByteCount(str);
            if (length < 256)
                return true;

            return false;
        }
    }
}
