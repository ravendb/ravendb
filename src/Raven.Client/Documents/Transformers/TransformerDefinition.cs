using System;

namespace Raven.Client.Documents.Transformers
{
    public class TransformerDefinition
    {
        /// <summary>
        /// Transformer etag (internal).
        /// </summary>
        public long Etag { get; set; }

        /// <summary>
        /// Projection function.
        /// </summary>
        public string TransformResults { get; set; }

        /// <summary>
        /// Transformer name.
        /// </summary>
        public string Name { get; set; }

        public TransformerLockMode LockMode { get; set; }

        public bool Equals(TransformerDefinition other)
        {
            if (ReferenceEquals(null, other))
                return false;

            if (ReferenceEquals(this, other))
                return true;

            var result = Compare(other);

            return result == TransformerDefinitionCompareDifferences.None;
        }

        public TransformerDefinitionCompareDifferences Compare(TransformerDefinition other)
        {
            if (other == null)
                return TransformerDefinitionCompareDifferences.All;

            var result = TransformerDefinitionCompareDifferences.None;
            if (Etag != other.Etag)
                result |= TransformerDefinitionCompareDifferences.Etag;

            if (TransformResults != other.TransformResults)
                result |= TransformerDefinitionCompareDifferences.TransformResults;

            if (LockMode != other.LockMode)
                result |= TransformerDefinitionCompareDifferences.LockMode;

            return result;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TransformerDefinition)obj);
        }

        public override int GetHashCode()
        {
            return TransformResults?.GetHashCode() ?? 0;
        }

        public TransformerDefinition Clone()
        {
            return (TransformerDefinition)MemberwiseClone();
        }

        public override string ToString()
        {
            return TransformResults;
        }
    }

    [Flags]
    public enum TransformerDefinitionCompareDifferences
    {
        None = 0,
        Etag = 1 << 0,
        TransformResults = 1 << 1,
        LockMode = 1 << 2,

        All = Etag | TransformResults | LockMode
    }
}
