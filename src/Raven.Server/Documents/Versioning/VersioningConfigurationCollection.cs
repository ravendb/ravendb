namespace Raven.Server.Documents.Versioning
{
    public class VersioningConfigurationCollection
    {
        public long? MaxRevisions { get; set; }

        public bool Active { get; set; }

        public bool PurgeOnDelete { get; set; }

        protected bool Equals(VersioningConfigurationCollection other)
        {
            return MaxRevisions == other.MaxRevisions && Active == other.Active && PurgeOnDelete == other.PurgeOnDelete;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((VersioningConfigurationCollection)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = MaxRevisions.GetHashCode();
                hashCode = (hashCode * 397) ^ Active.GetHashCode();
                hashCode = (hashCode * 397) ^ PurgeOnDelete.GetHashCode();
                return hashCode;
            }
        }
    }
}