using System;
using Raven.Client.Documents.Indexes;

namespace Raven.Server.Documents.Indexes
{
    public class IndexField
    {
        public string Name { get; set; }

        public string Analyzer { get; set; }

        public SortOptions? Sort { get; set; }

        public AggregationOperation Aggregation { get; set; }

        public FieldStorage Storage { get; set; }

        public FieldIndexing Indexing { get; set; }

        public FieldTermVector TermVector { get; set; }
        
        public bool HasSuggestions { get; set; }

        public IndexField()
        {
            Indexing = FieldIndexing.Default;
            Storage = FieldStorage.No;
        }

        public static IndexField Create(string name, IndexFieldOptions options, IndexFieldOptions allFields)
        {
            var field = new IndexField
            {
                Name = name,
                Analyzer = options.Analyzer ?? allFields?.Analyzer
            };
            
            if (options.Indexing.HasValue)
                field.Indexing = options.Indexing.Value;
            else if (string.IsNullOrWhiteSpace(field.Analyzer) == false)
                field.Indexing = FieldIndexing.Analyzed;

            if (options.Sort.HasValue)
                field.Sort = options.Sort.Value;
            else if (allFields?.Sort != null)
                field.Sort = allFields.Sort.Value;

            if (options.Storage.HasValue)
                field.Storage = options.Storage.Value;
            else if (allFields?.Storage != null)
                field.Storage = allFields.Storage.Value;

            if (options.TermVector.HasValue)
                field.TermVector = options.TermVector.Value;
            else if (allFields?.TermVector != null)
                field.TermVector = allFields.TermVector.Value;

            if (options.Suggestions.HasValue)
                field.HasSuggestions = options.Suggestions.Value;
                        
            // options.Spatial // TODO [ppekrol]

            return field;
        }

        protected bool Equals(IndexField other)
        {
            return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase)
                && string.Equals(Analyzer, other.Analyzer, StringComparison.OrdinalIgnoreCase)
                && Sort == other.Sort
                && Aggregation == other.Aggregation
                && Storage == other.Storage
                && Indexing == other.Indexing
                && TermVector == other.TermVector;
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
            return Equals((IndexField)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Name) : 0);
                hashCode = (hashCode * 397) ^ (Analyzer != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Analyzer) : 0);
                hashCode = (hashCode * 397) ^ Sort.GetHashCode();
                hashCode = (hashCode * 397) ^ (int)Aggregation;
                hashCode = (hashCode * 397) ^ (int)Storage;
                hashCode = (hashCode * 397) ^ (int)Indexing;
                hashCode = (hashCode * 397) ^ (int)TermVector;
                hashCode = (hashCode * 397) ^ (HasSuggestions ? 233 : 343);
                return hashCode;
            }
        }

        public IndexFieldOptions ToIndexFieldOptions()
        {
            return new IndexFieldOptions
            {
                Analyzer = Analyzer,
                Indexing = Indexing,
                Sort = Sort,
                Storage = Storage,
                TermVector = TermVector
            };
        }
    }
}