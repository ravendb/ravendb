namespace Raven.Abstractions.Indexing
{
    public class TransformerDefinition
    {
        /// <summary>
        /// Projection function.
        /// </summary>
        public string TransformResults { get; set; }

        /// <summary>
        /// Transformer identifier (internal).
        /// </summary>
        public int TransfomerId { get; set; }

        /// <summary>
        /// Transformer name.
        /// </summary>
        public string Name { get; set; }

        public TransformerLockMode LockMode { get; set; }

        /// <summary>
        /// Transformer version, used in transformer replication in order to identify if two transformers are indeed the same.
        /// </summary>
        public int? TransformerVersion { get; set; }

        public bool Equals(TransformerDefinition other)
        {
            return string.Equals(TransformResults, other.TransformResults);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((TransformerDefinition) obj);
        }

        public override int GetHashCode()
        {
            return (TransformResults != null ? TransformResults.GetHashCode() : 0);
        }

        public TransformerDefinition Clone()
        {
            return (TransformerDefinition) MemberwiseClone();
        }

        public override string ToString()
        {
            return TransformResults;
        }
    }
}
