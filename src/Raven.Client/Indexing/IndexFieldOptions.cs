using System;

using Raven.Abstractions.Indexing;

namespace Raven.Client.Indexing
{
    public class IndexFieldOptions
    {
        public FieldStorage? Storage { get; set; }

        public FieldIndexing? Indexing { get; set; }

        public SortOptions? Sort { get; set; }

        public FieldTermVector? TermVector { get; set; }

        public SpatialOptions Spatial { get; set; }

        public string Analyzer { get; set; }

        public bool? Suggestions { get; set; }

        protected bool Equals(IndexFieldOptions other)
        {
            return Storage == other.Storage
                   && Indexing == other.Indexing
                   && Sort == other.Sort
                   && TermVector == other.TermVector
                   && Equals(Spatial, other.Spatial)
                   && string.Equals(Analyzer, other.Analyzer, StringComparison.OrdinalIgnoreCase)
                   && Suggestions == other.Suggestions;
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
                hashCode = (hashCode * 397) ^ Sort.GetHashCode();
                hashCode = (hashCode * 397) ^ TermVector.GetHashCode();
                hashCode = (hashCode * 397) ^ (Spatial?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Analyzer != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Analyzer) : 0);
                hashCode = (hashCode * 397) ^ Suggestions.GetHashCode();
                return hashCode;
            }
        }
    }
}