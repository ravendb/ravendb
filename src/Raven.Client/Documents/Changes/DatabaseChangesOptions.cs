using System;

namespace Raven.Client.Documents.Changes
{
    public class DatabaseChangesOptions
    {
        public string DatabaseName { get; set; }

        public string NodeTag { get; set; }

        protected bool Equals(DatabaseChangesOptions other)
        {
            return string.Equals(DatabaseName, other.DatabaseName, StringComparison.OrdinalIgnoreCase) && string.Equals(NodeTag, other.NodeTag, StringComparison.OrdinalIgnoreCase);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((DatabaseChangesOptions)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((DatabaseName != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(DatabaseName) : 0) * 397) ^ (NodeTag != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(NodeTag) : 0);
            }
        }
    }
}
