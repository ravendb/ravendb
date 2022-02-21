using System;
using System.Collections.Concurrent;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ShardedHandlers.ContinuationTokens
{
    public abstract class ContinuationToken : IDynamicJson
    {
        public const string ContinuationTokenQueryString = "continuation-token";
        public const string PropertyName = "ContinuationToken";
        private static ConcurrentDictionary<Type, Func<BlittableJsonReaderObject, ContinuationToken>> ConverterCache =
            new ConcurrentDictionary<Type, Func<BlittableJsonReaderObject, ContinuationToken>>();
        public string ToBase64(JsonOperationContext context)
        {
            var json = context.ReadObject(ToJson(), $"token for {GetType()}");
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(json.ToString()));
        }

        public abstract DynamicJsonValue ToJson();

        public static T FromBase64<T>(JsonOperationContext context, string token) where T : ContinuationToken
        {
            if (token == null)
                return default;

            var bytes = Convert.FromBase64String(token);
            token = Encoding.UTF8.GetString(bytes);

            using (var json = JsonStringToBlittable(context, token))
            {
                var converter = ConverterCache.GetOrAdd(typeof(T), (_) => JsonDeserializationBase.GenerateJsonDeserializationRoutine<T>());

                return (T)converter(json);
            }
        }

        private static unsafe BlittableJsonReaderObject JsonStringToBlittable(JsonOperationContext context, string json)
        {
            var maxLength = Encoding.UTF8.GetMaxByteCount(json.Length);
            Span<byte> byteSpan = stackalloc byte[maxLength];
            var length = Encoding.UTF8.GetBytes(json.AsSpan(), byteSpan);
            fixed (byte* ptr = byteSpan)
            {
                var blittableJson = context.ParseBuffer(ptr, length, "ConvertContinuationToken", BlittableJsonDocumentBuilder.UsageMode.None);
                blittableJson.BlittableValidation(); //precaution, needed because this is user input..
                return blittableJson;
            }
        }
    }

}
