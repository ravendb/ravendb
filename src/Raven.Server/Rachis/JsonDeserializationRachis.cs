using System;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    internal sealed class JsonDeserializationRachis<T> : JsonDeserializationBase
    {
        public static Func<BlittableJsonReaderObject,T> Deserialize = 
            GenerateJsonDeserializationRoutine<T>();
    }
}
