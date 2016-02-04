using System;
namespace Raven.Abstractions.Data
{
    public class DocumentsChanges
    {
        /// <summary>
        /// Previous field value.
        /// </summary>
        public string FieldOldValue { get; set; }

        /// <summary>
        /// Current field value.
        /// </summary>
        public string FieldNewValue { get; set; }

        /// <summary>
        /// Previous field type.
        /// </summary>
        public string FieldOldType { get; set; }

        /// <summary>
        /// Current field type.
        /// </summary>
        public string FieldNewType { get; set; }

        /// <summary>
        /// Type of change that occured.
        /// </summary>
        public ChangeType Change { get; set; }

        /// <summary>
        /// Name of field on which the change occured.
        /// </summary>
        public string FieldName { get; set; }

        public enum ChangeType
        {
            DocumentDeleted,
            DocumentAdded,
            FieldChanged,
            NewField,
            RemovedField,
            ArrayValueAdded,
            ArrayValueRemoved
        }

        protected bool Equals(DocumentsChanges other)
        {
            return string.Equals(FieldOldValue, other.FieldOldValue)
                   && string.Equals(FieldNewValue, other.FieldNewValue)
                   && string.Equals(FieldOldType, other.FieldOldType)
                   && string.Equals(FieldName, other.FieldName)
                   && string.Equals(FieldNewType, other.FieldNewType)
                   && Change ==  other.Change;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (FieldOldValue != null ? FieldOldValue.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (FieldNewValue != null ? FieldNewValue.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (FieldOldType != null ? FieldOldType.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (FieldNewType != null ? FieldNewType.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (FieldName != null ? FieldName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Change.GetHashCode());
                return hashCode;
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((DocumentsChanges) obj);
        }
    }
    
}
