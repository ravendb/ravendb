using System;
using Sparrow.Json;

namespace Raven.Server.Rachis
{
    public class JsonDeserializationRachis : JsonDeserializationBase
    {


        public static readonly Func<BlittableJsonReaderObject, RachisHello> RachisHello =
            GenerateJsonDeserializationRoutine<RachisHello>();

        public static readonly Func<BlittableJsonReaderObject, RequestVote> RequestVote =
            GenerateJsonDeserializationRoutine<RequestVote>();

        public static readonly Func<BlittableJsonReaderObject, AppendEntries> AppendEntries =
            GenerateJsonDeserializationRoutine<AppendEntries>();

        public static readonly Func<BlittableJsonReaderObject, InstallSnapshot> InstallSnapshot =
            GenerateJsonDeserializationRoutine<InstallSnapshot>();

        public static readonly Func<BlittableJsonReaderObject, RachisEntry> RachisEntry =
            GenerateJsonDeserializationRoutine<RachisEntry>();

    }
}