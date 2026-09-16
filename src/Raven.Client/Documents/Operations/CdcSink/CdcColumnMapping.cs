using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink;

/// <summary>
/// Maps a single SQL column to a RavenDB document property or attachment.
/// </summary>
public class CdcColumnMapping : IDynamicJson
{
    /// <summary>
    /// The SQL column name in the source table.
    /// </summary>
    public string Column { get; set; }

    /// <summary>
    /// The target name in RavenDB. For Default and Json types, this is the
    /// document property name. For Attachment type, this is the attachment name.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// How this column is stored. Default stores as a document property with
    /// standard type conversion. Json parses the value as a native JSON object
    /// or array. Attachment stores the raw value as a RavenDB attachment.
    /// </summary>
    public CdcColumnType Type { get; set; }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Column)] = Column,
            [nameof(Name)] = Name,
        };

        if (Type != CdcColumnType.Default)
            json[nameof(Type)] = Type.ToString();

        return json;
    }
}
