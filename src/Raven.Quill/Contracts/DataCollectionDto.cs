namespace Raven.Quill.Contracts;

/// <summary>
/// A mirrored data collection — the prototype's <c>DataCollection</c>. Phase-1
/// scope: collection name + current document count. <paramref name="Fields"/>
/// ships empty (RavenDB is schemaless — per-field stats / embedding config are a
/// later enhancement). System collections (<c>@</c>-prefixed) are excluded.
/// </summary>
public sealed record DataCollectionDto(string AppId, string Name, long DocumentsCount, object[] Fields);
