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
        /// Previous field type.
        /// </summary>
        [Obsolete("DocumentsChanges.FieldOldType is not supported anymore. Will be removed in next major version of the product.")]
        public BlittableJsonToken FieldOldType { get; set; }

        /// <summary>
        /// Current field type.
        /// </summary>
        [Obsolete("DocumentsChanges.FieldNewType is not supported anymore. Will be removed in next major version of the product.")]
        public BlittableJsonToken FieldNewType { get; set; }

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
            ArrayValueRemoved,
            [Obsolete("ChangeType.FieldTypeChanged is not supported anymore. Will be removed in next major version of the product.")]
            FieldTypeChanged,
            [Obsolete("ChangeType.EntityTypeChanged is not supported anymore. Will be removed in next major version of the product.")]
            EntityTypeChanged
        }

        protected bool Equals(DocumentsChanges other)
        {
            return Equals(FieldOldValue, other.FieldOldValue)
                   && Equals(FieldNewValue, other.FieldNewValue)
                   && string.Equals(FieldName, other.FieldName)
                   && string.Equals(FieldPath, other.FieldPath)
#pragma warning disable 618
                   && Equals(FieldOldType, other.FieldOldType)
                   && Equals(FieldNewType, other.FieldNewType)
#pragma warning restore 618
                   && Change == other.Change;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = FieldOldValue?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (FieldNewValue?.GetHashCode() ?? 0);
#pragma warning disable 618
                hashCode = (hashCode * 397) ^ (FieldOldType.GetHashCode());
                hashCode = (hashCode * 397) ^ (FieldNewType.GetHashCode());
#pragma warning restore 618
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
