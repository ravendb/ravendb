using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public class DocumentsChanges
    {
        /// <summary>
        /// Previous field value.
        /// </summary>
        public object FieldOldValue { get; set; }

        /// <summary>
        /// Current field value.
        /// </summary>
        public object FieldNewValue { get; set; }

        /// <summary>
        /// Type of change that occurred.
        /// </summary>
        public ChangeType Change { get; set; }

        /// <summary>
        /// Name of field on which the change occurred.
        /// </summary>
        public string FieldName { get; set; }

        /// <summary>
        /// Path of field on which the change occurred.
        /// </summary>
        public string FieldPath { get; set; }

        /// <summary>
        /// Path + Name of field on which the change occurred.
        /// </summary>
        public string FieldFullName => string.IsNullOrEmpty(FieldPath) ? FieldName : FieldPath + "." + FieldName;

        public enum ChangeType
        {
            DocumentDeleted,
            DocumentAdded,
            FieldChanged,
            NewField,
            RemovedField,
            ArrayValueChanged,
            ArrayValueAdded,
            ArrayValueRemoved
        }

        protected bool Equals(DocumentsChanges other)
        {
            return Equals(FieldOldValue, other.FieldOldValue)
                   && Equals(FieldNewValue, other.FieldNewValue)
                   && string.Equals(FieldName, other.FieldName)
                   && string.Equals(FieldPath, other.FieldPath)
                   && Change == other.Change;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = FieldOldValue?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (FieldNewValue?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (FieldName?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (FieldPath?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Change.GetHashCode());
                return hashCode;
            }
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((DocumentsChanges)obj);
        }
    }

}
