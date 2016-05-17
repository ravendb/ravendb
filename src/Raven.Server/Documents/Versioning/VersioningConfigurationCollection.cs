namespace Raven.Server.Documents.Versioning
{
    public class VersioningConfigurationCollection
    {
        public int? MaxRevisions { get; set; }

        public bool Active { get; set; }

        public bool PurgeOnDelete { get; set; }
    }
}