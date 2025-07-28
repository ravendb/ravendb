using System;
using Raven.Client.Documents.Attachments;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments;

public class RetireAttachmentParameters : IDynamicJson
{
    public RetireAttachmentParameters()
    {
        // Parameterless constructor for serialization

        //TODO: egor make this ctor internal when we fix failing tests in DocumentSessionRetiredAttachmentsAsyncTests
    }

    public RetireAttachmentParameters(string identifier, DateTime at)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentNullException(nameof(identifier), "Attachment identifier cannot be null or whitespace.");
        if (at == default)
            throw new ArgumentException("Attachment retirement date cannot be default value.", nameof(at));
        Identifier = identifier;
        At = at;
    }

    public DateTime At { get; set; }
    public string Identifier { get; set; }
    public RetiredAttachmentFlags Flags { get; set; }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(At)] = At,
            [nameof(Identifier)] = Identifier,
            [nameof(Flags)] = Flags.ToString()
        };
        return json;
    }
}