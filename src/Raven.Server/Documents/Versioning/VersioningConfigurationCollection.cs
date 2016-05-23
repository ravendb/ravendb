namespace Raven.Server.Documents.Versioning
{
    public class VersioningConfigurationCollection
    {
        public long? MaxRevisions { get; set; }

        public bool Active { get; set; }

        public bool PurgeOnDelete { get; set; }
    }
}