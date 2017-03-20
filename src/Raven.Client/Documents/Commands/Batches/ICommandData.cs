using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public interface ICommandData
    {
        string Key { get; }

        long? Etag { get; }

        HttpMethod Method { get; }

        DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context);
    }
}