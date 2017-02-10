using System.Net.Http;
using Sparrow.Json.Parsing;

namespace Raven.Client.Data.Commands
{
    public interface ICommandData
    {
        string Key { get; }

        long? Etag { get; }

        HttpMethod Method { get; }

        DynamicJsonValue ToJson();
    }
}