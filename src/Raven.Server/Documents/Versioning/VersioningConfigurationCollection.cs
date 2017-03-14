namespace Raven.Server.Documents.Versioning
{
#pragma warning disable CS0659 // Type overrides Object.Equals(object o) but does not override Object.GetHashCode()
    public class VersioningConfigurationCollection
#pragma warning restore CS0659 // Type overrides Object.Equals(object o) but does not override Object.GetHashCode()
    {
        public long? MaxRevisions { get; set; }

        public bool Active { get; set; }

        public bool PurgeOnDelete { get; set; }

#pragma warning disable 659
        public override bool Equals(object obj)
#pragma warning restore 659
        {
            var other = obj as VersioningConfigurationCollection;
            if (other == null)
                return false;
            if (MaxRevisions != other.MaxRevisions)
                return false;
            if (Active != other.Active)
                return false;
            if (PurgeOnDelete != other.PurgeOnDelete)
                return false;
            return true;
        }
    }
}