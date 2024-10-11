using System;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;

namespace Raven.Client.Documents.Indexes
{
    public sealed class IndexFieldOptions
    {
        public FieldStorage? Storage { get; set; }

        public FieldIndexing? Indexing { get; set; }

        public FieldTermVector? TermVector { get; set; }

        public SpatialOptions Spatial { get; set; }
        
        public VectorOptions Vector { get; set; }

        public string Analyzer { get; set; }

        public bool? Suggestions { get; set; }

        private bool Equals(IndexFieldOptions other)
        {
            return Storage == other.Storage
                   && Indexing == other.Indexing
                   && TermVector == other.TermVector
                   && Equals(Spatial, other.Spatial)
                   && string.Equals(Analyzer, other.Analyzer, StringComparison.OrdinalIgnoreCase)
                   && Suggestions == other.Suggestions
                   && Vector == other.Vector;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != GetType())
            {
                return false;
            }
            return Equals((IndexFieldOptions)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Storage.GetHashCode();
                hashCode = (hashCode * 397) ^ Indexing.GetHashCode();
                hashCode = (hashCode * 397) ^ TermVector.GetHashCode();
                hashCode = (hashCode * 397) ^ (Spatial?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Analyzer != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Analyzer) : 0);
                hashCode = (hashCode * 397) ^ Suggestions.GetHashCode();
                hashCode = (hashCode * 397) ^ (Vector?.GetHashCode() ?? 0);
                return hashCode;
            }
        }
    }
}
