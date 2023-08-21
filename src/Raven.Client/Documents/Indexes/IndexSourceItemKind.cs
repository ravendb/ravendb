namespace Raven.Client.Documents.Indexes;

public enum IndexSourceItemKind
{
    Default, // only unarchived documents will be processed by the index
    ArchivedOnly, // only archived docs will be processed by the index
    ArchivedIncluded // all documents will be processed by the index
}
