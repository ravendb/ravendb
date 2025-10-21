using System;
using Raven.Client.Documents.Attachments;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Attachments;

/// <summary>
/// Parameters for retiring an attachment in RavenDB.
/// </summary>
public class RetireAttachmentParameters : IDynamicJson
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RetireAttachmentParameters"/> class.
    /// Parameterless constructor for serialization purposes.
    /// </summary>
    public RetireAttachmentParameters()
    {
        // Parameterless constructor for serialization
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RetireAttachmentParameters"/> class with the specified identifier and retirement date.
    /// </summary>
    /// <param name="identifier">The unique identifier of the attachment to retire. Cannot be null or whitespace.</param>
    /// <param name="at">The date and time when the attachment should be retired. Cannot be default value.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="identifier"/> is null or whitespace.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="at"/> is the default DateTime value.</exception>
    public RetireAttachmentParameters(string identifier, DateTime at)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentNullException(nameof(identifier), "Attachment identifier cannot be null or whitespace.");
        if (at == default)
            throw new ArgumentException("Attachment retirement date cannot be default value.", nameof(at));
        Identifier = identifier;
        At = at;
    }

    /// <summary>
    /// Gets or sets the date and time when the attachment should be retired.
    /// </summary>
    public DateTime At { get; set; }

    /// <summary>
    /// Gets or sets the unique identifier of the attachment to retire.
    /// </summary>
    public string Identifier { get; set; }

    /// <summary>
    /// Gets or sets the flags that control the retirement behavior of the attachment.
    /// </summary>
    internal RetiredAttachmentFlags Flags { get; set; }

    /// <summary>
    /// Converts the current instance to a <see cref="DynamicJsonValue"/> for serialization.
    /// </summary>
    /// <returns>A <see cref="DynamicJsonValue"/> representation of this instance.</returns>
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
