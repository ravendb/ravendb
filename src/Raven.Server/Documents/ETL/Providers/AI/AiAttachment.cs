using System;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiAttachment
{
    public string Name { get; set; }
    public string Type { get; set; }
    public AiAttachmentSource Source { get; set; }
    public string Data { get; set; }
    
    /// <summary>
    /// For <see cref="AiAttachmentSource.Deferred"/>, the storage to retrieve from.
    /// </summary>
    public string RemoteStorageId { get; set; }
    public long DownloadDurationInMs { get; set; }
    
    public AiAttachment()
    {
        // for deserialization
    }

    public AiAttachment(string name, string type, AiAttachmentSource source, string dataAsBase64, string remoteStorageId = null)
    {
        ValidationMethods.AssertNotNullOrEmpty(name, nameof(Name));
        ValidationMethods.AssertNotNullOrEmpty(type, nameof(Type));
        if (source != AiAttachmentSource.NotFound && source != AiAttachmentSource.Deferred)
            ValidationMethods.AssertNotNullOrEmpty(dataAsBase64, nameof(Data));

        Name = name;
        Type = type;
        Source = source;
        Data = dataAsBase64;
        RemoteStorageId = remoteStorageId;
    }

    public DynamicJsonValue ToJson()
    {
        var json = new DynamicJsonValue
        {
            [nameof(Name)] = Name,
            [nameof(Type)] = Type,
            [nameof(Source)] = Source,
            [nameof(Data)] = Data,
            [nameof(RemoteStorageId)] = RemoteStorageId,
            [nameof(DownloadDurationInMs)] = DownloadDurationInMs
        };

        return json;
    }
}

public enum AiAttachmentSource
{
    FromAttachment, 
    FromScript, 
    NotFound,

    /// <summary>
    /// For attachments to be resolved later (e.g., remote attachments)
    /// </summary>
    Deferred
}
