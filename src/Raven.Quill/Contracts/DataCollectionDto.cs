namespace Raven.Quill.Contracts;

public sealed record DataCollectionDto(string AppId, string Name, long DocumentsCount, object[] Fields);
