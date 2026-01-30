using System;
using Raven.Client.Documents.Attachments;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments;

/// <summary>
/// Represents the scheduling parameters for uploading an attachment to remote storage in RavenDB.
/// </summary>
/// <remarks>
/// This type is used when instructing RavenDB to perform an upload to remote cloud storage of an attachment at a specified time. The <see cref="At"/> value should normally be expressed in UTC.
/// </remarks>
/// <example>
/// var p = new RemoteAttachmentParameters(identifier: "s3-storage", at: DateTime.UtcNow.AddMinutes(5));
/// </example>
public class RemoteAttachmentParameters : IDynamicJson
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RemoteAttachmentParameters"/> class.
    /// Parameterless constructor for serialization purposes.
    /// </summary>
    public RemoteAttachmentParameters()
    {
        // Parameterless constructor for serialization
    }

    /// <summary>   
    /// Initializes a new instance of the <see cref="RemoteAttachmentParameters"/> class with the specified destination identifier and scheduled remote upload time.
    /// </summary>
    /// <param name="identifier">
    /// A unique identifier specifying the remote destination for uploading the attachment.
    /// </param>
    /// <param name="at">
    /// The (usually UTC) date and time at which the remote upload should be executed. Must not be the default <see cref="DateTime"/> value.
    /// </param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="identifier"/> is null or whitespace.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="at"/> is the default DateTime value.</exception>
    public RemoteAttachmentParameters(string identifier, DateTime at)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentNullException(nameof(identifier), "Attachment identifier cannot be null or whitespace.");
        if (at == default)
            throw new ArgumentException("Attachment upload date cannot be default value.", nameof(at));
        Identifier = identifier;
        At = at;
    }

    /// <summary>
    /// Gets or sets the scheduled (preferably UTC) date and time when the attachment should be uploaded to the remote destination.
    /// </summary>
    public DateTime At { get; set; }

    /// <summary>
    /// Gets or sets the identifier of the remote storage destination to which the attachment should be uploaded.
    /// </summary>
    public string Identifier { get; set; }

    /// <summary>
    /// Gets or sets flags controlling the remote upload behavior.
    /// Use <see cref="RemoteAttachmentFlags.Remote"/> to mark the attachment for remote handling.
    /// </summary>
    [ForceJsonSerialization]
    internal RemoteAttachmentFlags Flags { get; set; }

    /// <summary>
    /// Converts this instance into a <see cref="DynamicJsonValue"/> suitable for RavenDB internal serialization.
    /// The <see cref="Flags"/> value is emitted as its string representation.
    /// </summary>
    /// <returns>A <see cref="DynamicJsonValue"/> representing this instance.</returns>
    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(At)] = At,
            [nameof(Identifier)] = Identifier,
            [nameof(Flags)] = Flags.ToString()
        };
    }
}
