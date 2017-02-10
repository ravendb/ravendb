using System;
using System.Net.Http;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Data.Commands
{
    public class PatchCommandData : ICommandData
    {
        public PatchCommandData(string id, long? etag, PatchRequest patch, PatchRequest patchIfMissing)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (patch == null)
                throw new ArgumentNullException(nameof(patch));

            Key = id;
            Etag = etag;
            Patch = patch;
            PatchIfMissing = patchIfMissing;
        }

        public string Key { get; }
        public long? Etag { get; }
        public PatchRequest Patch { get; }
        public PatchRequest PatchIfMissing { get; }
        public HttpMethod Method => HttpMethods.Patch;

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(Key)] = Key,
                [nameof(Etag)] = Etag,
                [nameof(Patch)] = Patch.ToJson(),
                [nameof(Method)] = Method.Method
            };

            if (PatchIfMissing != null)
                json[nameof(PatchIfMissing)] = PatchIfMissing?.ToJson();

            return json;
        }
    }
}