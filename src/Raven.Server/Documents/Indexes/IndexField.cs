using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexFieldBase
    {
        public string Name { get; set; }

        public FieldStorage Storage { get; set; }

        public bool HasSuggestions { get; set; }

        public T As<T>() where T : IndexFieldBase
        {
            return this as T;
        }
    }

    public class IndexField : IndexFieldBase
    {
        internal string OriginalName { get; set; }

        public string Analyzer { get; set; }

        public FieldIndexing Indexing { get; set; }

        public FieldTermVector TermVector { get; set; }

        public SpatialOptions Spatial { get; set; }

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
                field.Indexing = FieldIndexing.Search;
            else if (allFields?.Indexing != null)
                field.Indexing = allFields.Indexing.Value;

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
            else if (allFields?.Suggestions != null)
                field.HasSuggestions = allFields.Suggestions.Value;

            if (options.Spatial != null)
                field.Spatial = new SpatialOptions(options.Spatial);

            return field;
        }

        protected bool Equals(IndexField other)
        {
            return string.Equals(Name, other.Name, StringComparison.Ordinal)
                && string.Equals(Analyzer, other.Analyzer, StringComparison.Ordinal)
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
                var hashCode = (Name != null ? StringComparer.Ordinal.GetHashCode(Name) : 0);
                hashCode = (hashCode * 397) ^ (Analyzer != null ? StringComparer.Ordinal.GetHashCode(Analyzer) : 0);
                hashCode = (hashCode * 397) ^ (int)Storage;
                hashCode = (hashCode * 397) ^ (int)Indexing;
                hashCode = (hashCode * 397) ^ (int)TermVector;
                hashCode = (hashCode * 397) ^ (HasSuggestions ? 233 : 343);
                return hashCode;
            }
        }

        private IndexFieldOptions _indexFieldOptions;

        public IndexFieldOptions ToIndexFieldOptions()
        {
            if (_indexFieldOptions != null)
                return _indexFieldOptions;

            return _indexFieldOptions = new IndexFieldOptions
            {
                Analyzer = Analyzer,
                Indexing = Indexing,
                Storage = Storage,
                TermVector = TermVector,
                Suggestions = HasSuggestions,
                Spatial = Spatial
            };
        }
    }

    public class AutoIndexField : IndexFieldBase
    {
        public AggregationOperation Aggregation { get; set; }

        public GroupByArrayBehavior GroupByArrayBehavior { get; set; }

        public AutoIndexField()
        {
            Indexing = AutoFieldIndexing.Default;
            Storage = FieldStorage.No;
            GroupByArrayBehavior = GroupByArrayBehavior.NotApplicable;
        }

        public bool HasQuotedName { get; set; }

        public AutoFieldIndexing Indexing { get; set; }

        public AutoSpatialOptions Spatial { get; set; }

        public static AutoIndexField Create(string name, AutoIndexDefinition.AutoIndexFieldOptions options)
        {
            var field = new AutoIndexField
            {
                Name = name,
                HasQuotedName = options.IsNameQuoted
            };

            if (options.Indexing.HasValue)
                field.Indexing = options.Indexing.Value;

            if (options.Storage.HasValue)
                field.Storage = options.Storage.Value;

            if (options.Spatial != null)
                field.Spatial = new AutoSpatialOptions(options.Spatial);

            if (options.Suggestions.HasValue)
                field.HasSuggestions = options.Suggestions.Value;

            field.Aggregation = options.Aggregation;
            field.GroupByArrayBehavior = options.GroupByArrayBehavior;

            return field;
        }

        public List<IndexField> ToIndexFields()
        {
            var fields = new List<IndexField>();

            if (Spatial != null)
            {
                fields.Add(new IndexField
                {
                    Indexing = FieldIndexing.Default,
                    Name = Name,
                    Storage = Storage,
                    HasSuggestions = HasSuggestions,
                    Spatial = new AutoSpatialOptions(Spatial)
                });

                return fields;
            }

            var originalName = Name;

            if (HasQuotedName)
                originalName = $"'{originalName}'";

            fields.Add(new IndexField
            {
                Indexing = FieldIndexing.Default,
                Name = Name,
                OriginalName = HasQuotedName ? originalName : null,
                Storage = Storage,
                HasSuggestions = HasSuggestions
            });

            if (Indexing == AutoFieldIndexing.Default)
                return fields;

            var hasHighlighting = Indexing.HasFlag(AutoFieldIndexing.Highlighting);

            if (Indexing.HasFlag(AutoFieldIndexing.Search) || hasHighlighting)
            {
                fields.Add(new IndexField
                {
                    Indexing = FieldIndexing.Search,
                    Name = GetSearchAutoIndexFieldName(Name),
                    OriginalName = originalName,
                    Storage = hasHighlighting ? FieldStorage.Yes : Storage,
                    TermVector = hasHighlighting ? FieldTermVector.WithPositionsAndOffsets : FieldTermVector.No
                });
            }

            if (Indexing.HasFlag(AutoFieldIndexing.Exact))
            {
                fields.Add(new IndexField
                {
                    Indexing = FieldIndexing.Exact,
                    Name = GetExactAutoIndexFieldName(Name),
                    OriginalName = originalName,
                    Storage = Storage
                });
            }

            return fields;
        }

        protected bool Equals(AutoIndexField other)
        {
            return string.Equals(Name, other.Name, StringComparison.Ordinal)
                   && Storage == other.Storage
                   && Indexing == other.Indexing
                   && Aggregation == other.Aggregation;
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
            return Equals((AutoIndexField)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? StringComparer.Ordinal.GetHashCode(Name) : 0);
                hashCode = (hashCode * 397) ^ (int)Storage;
                hashCode = (hashCode * 397) ^ (int)Indexing;
                hashCode = (hashCode * 397) ^ (int)Aggregation;

                return hashCode;
            }
        }

        public static string GetSearchAutoIndexFieldName(string name)
        {
            return $"search({name})";
        }

        public static string GetExactAutoIndexFieldName(string name)
        {
            return $"exact({name})";
        }

        public static string GetHighlightingAutoIndexFieldName(string name)
        {
            return $"highlight({name})";
        }

        public static string GetGroupByArrayContentAutoIndexFieldName(string name)
        {
            return $"array({name})";
        }

        public static string GetSuggestionsAutoIndexFieldName(string name)
        {
            return $"suggest({name})";
        }
    }
}
