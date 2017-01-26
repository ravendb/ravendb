using System;
using System.Net.Http;
using Sparrow.Json.Parsing;

namespace Raven.NewClient.Client.Data.Commands
{
    public class DeleteCommandData : ICommandData
    {
        public DeleteCommandData(string id, long? etag)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            Key = id;
            Etag = etag;
        }

        public string Key { get; }
        public long? Etag { get; }
        public HttpMethod Method => HttpMethod.Delete;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Key)] = Key,
                [nameof(Etag)] = Etag,
                [nameof(Method)] = Method.Method
            };
        }
    }
}