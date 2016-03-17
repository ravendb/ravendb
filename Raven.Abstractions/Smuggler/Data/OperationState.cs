namespace Raven.Abstractions.Smuggler.Data
{
    public class OperationState : LastEtagsInfo
    {
        public string FilePath { get; set; }

        public int NumberOfExportedDocuments { get; set; }

        public int NumberOfExportedAttachments { get; set; }
    }
}
