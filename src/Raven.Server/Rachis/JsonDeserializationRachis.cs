using System;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    public class JsonDeserializationRachis<T> : JsonDeserializationBase
    {
        public static Func<BlittableJsonReaderObject,T> Deserialize = 
            GenerateJsonDeserializationRoutine<T>();
    }
}