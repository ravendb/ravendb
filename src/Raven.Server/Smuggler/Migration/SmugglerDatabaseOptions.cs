namespace Raven.Server.Smuggler.Migration
{
    public class SmugglerDatabaseOptions
    {
        public ItemType OperateOnTypes { get; set; }

        public int BatchSize { get; set; }

        public bool ExportDeletions { get; set; }

        public string StartDocsEtag { get; set; }

        public string StartAttachmentsEtag { get; set; }

        public string StartDocsDeletionEtag { get; set; }

        public string StartAttachmentsDeletionEtag { get; set; }
    }
}
