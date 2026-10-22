using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.AI;

/// <summary>
/// Controls how a conversation attachment is handled by the model.
/// </summary>
public class AttachmentOptions : IDynamicJson
{
    /// <summary>
    /// When <c>false</c>, the attachment is stored on the conversation document but is never sent to the model.
    /// Default is <c>true</c>.
    /// </summary>
    public bool SendToModel { get; set; } = true;

    /// <summary>
    /// When <c>false</c>, only the attachment name is sent to the model, which can retrieve the content on demand.
    /// Default is <c>true</c>.
    /// </summary>
    public bool SendImmediately { get; set; } = true;

    /// <summary>
    /// When <c>true</c>, the attachment content is kept in the conversation history and sent to the model on subsequent turns.
    /// Default is <c>false</c>.
    /// </summary>
    public bool PersistToModelHistory { get; set; }

    internal void Validate(string attachmentName)
    {
        if (PersistToModelHistory == false)
            return;

        if (SendToModel == false)
            throw new InvalidOperationException(
                $"Attachment '{attachmentName}' cannot set both {nameof(PersistToModelHistory)} and {nameof(SendToModel)} = false, an attachment that is not sent to the model cannot be kept in its history.");

        if (SendImmediately == false)
            throw new InvalidOperationException(
                $"Attachment '{attachmentName}' cannot set both {nameof(PersistToModelHistory)} and {nameof(SendImmediately)} = false, only an attachment that is sent immediately can be kept in the model history.");
    }

    internal AttachmentOptions Clone() =>
        new()
        {
            SendToModel = SendToModel,
            SendImmediately = SendImmediately,
            PersistToModelHistory = PersistToModelHistory
        };

    internal static AttachmentOptions FromBlittable(BlittableJsonReaderObject json)
    {
        foreach (var propertyName in json.GetPropertyNames())
        {
            if (propertyName is not (nameof(SendToModel) or nameof(SendImmediately) or nameof(PersistToModelHistory)))
                throw new InvalidOperationException(
                    $"Unknown attachment option '{propertyName}'. Supported options are {nameof(SendToModel)}, {nameof(SendImmediately)} and {nameof(PersistToModelHistory)}.");
        }

        return new AttachmentOptions
        {
            SendToModel = ReadFlag(json, nameof(SendToModel), defaultValue: true),
            SendImmediately = ReadFlag(json, nameof(SendImmediately), defaultValue: true),
            PersistToModelHistory = ReadFlag(json, nameof(PersistToModelHistory), defaultValue: false)
        };
    }

    private static bool ReadFlag(BlittableJsonReaderObject json, string name, bool defaultValue)
    {
        if (json.TryGetMember(name, out object value) == false)
            return defaultValue;

        if (value is bool flag)
            return flag;

        throw new InvalidOperationException($"Attachment option '{name}' must be true or false, but got: {value ?? "null"}.");
    }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(SendToModel)] = SendToModel,
            [nameof(SendImmediately)] = SendImmediately,
            [nameof(PersistToModelHistory)] = PersistToModelHistory
        };
    }
}
