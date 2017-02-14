using System.Net.Http;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public interface ICommandData
    {
        string Key { get; }

        long? Etag { get; }

        HttpMethod Method { get; }

        DynamicJsonValue ToJson();
    }
}