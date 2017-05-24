using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands.Batches
{
    public interface ICommandData
    {
        string Id { get; }

        long? Etag { get; }

        CommandType Type { get; }

        DynamicJsonValue ToJson(DocumentConventions conventions, JsonOperationContext context);
    }

    public enum CommandType
    {
        None,
        PUT,
        PATCH,
        DELETE,
        AttachmentPUT,
        AttachmentDELETE,
    }
}