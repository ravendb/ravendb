using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments;

/// <summary>
/// Represents the basic information of an attachment, including its name, hash, content type, and size.
/// </summary>
/// <remarks>
/// This class serves as a base for more detailed attachment information, providing essential properties
/// for managing attachments within the database.
/// </remarks>
public class AttachmentName
{
    /// <summary>
    /// The name of the attachment.
    /// </summary>
    public string Name;

    /// <summary>
    /// The hash of the attachment content for integrity verification.
    /// </summary>
    public string Hash;

    /// <summary>
    /// The MIME type of the attachment.
    /// </summary>
    public string ContentType;

    /// <summary>
    /// The size of the attachment in bytes.
    /// </summary>
    public long Size;

    public RemoteAttachmentParameters RemoteParameters;

    internal virtual DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Hash)] = Hash,
            [nameof(ContentType)] = ContentType,
            [nameof(Size)] = Size
        };

        if (RemoteParameters != null)
            json[nameof(RemoteParameters)] = RemoteParameters.ToJson();

        return json;
    }
}
