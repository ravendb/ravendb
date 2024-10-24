using System;
using System.Collections.Generic;
using System.Numerics;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Utils;

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

    public sealed class IndexField : IndexFieldBase
    {
        internal string OriginalName { get; set; }

        /// <summary>
        ///  Corax field id.
        /// </summary>
        public int Id { get; set; }

        public string Analyzer { get; set; }

        public FieldIndexing Indexing { get; set; }

        public FieldTermVector TermVector { get; set; }

        public SpatialOptions Spatial { get; set; }
        
        public VectorOptions Vector { get; set; }

        public IndexField()
        {
            Indexing = FieldIndexing.Default;
            Storage = FieldStorage.No;
        }

        public static IndexField Create(string name, IndexFieldOptions options, IndexFieldOptions allFields, int id = 0)
        {
            var field = new IndexField
            {
                Name = name,
                Analyzer = options.Analyzer ?? allFields?.Analyzer,
                Id = id
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

            if (options.Vector != null)
                field.Vector = new(options.Vector);

            return field;
        }

        private bool Equals(IndexField other)
        {
            return Id == other.Id
                && string.Equals(Name, other.Name, StringComparison.Ordinal)
                && string.Equals(Analyzer, other.Analyzer, StringComparison.Ordinal)
                && Storage == other.Storage
                && Indexing == other.Indexing
                && SpatialOptions.Equals(Spatial, other.Spatial)
                && VectorOptions.Equals(Vector, other.Vector)
                && TermVector == other.TermVector
                //todo: should we add spatial & vector as well here?
                ;
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
                hashCode = (hashCode * 397) ^ Id;
                hashCode = (hashCode * 397) ^ (int)Indexing;
                hashCode = (hashCode * 397) ^ (int)TermVector;
                hashCode = (hashCode * 397) ^ (HasSuggestions ? 233 : 343);
                hashCode = (hashCode * 397) ^ (Spatial?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Vector?.GetHashCode() ?? 0);
                //todo: vector & spatial as well?
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
                Spatial = Spatial,
                Vector = Vector
            };
        }
    }

    public sealed class AutoIndexField : IndexFieldBase
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

        public int Id { get; set; }

        public AutoSpatialOptions Spatial { get; set; }
        
        public AutoVectorOptions Vector { get; set; }

        public bool SamePathToArrayAsGroupByField { get; set; }

        public static AutoIndexField Create(string name, AutoIndexDefinition.AutoIndexFieldOptions options)
        {
            var field = new AutoIndexField
            {
                Name = name,
                HasQuotedName = options.IsNameQuoted,
            };

            if (options.Indexing.HasValue)
                field.Indexing = options.Indexing.Value;

            if (options.Storage.HasValue)
                field.Storage = options.Storage.Value;

            if (options.Spatial != null)
                field.Spatial = new AutoSpatialOptions(options.Spatial);

            if (options.Vector != null)
                field.Vector = new AutoVectorOptions(options.Vector);

            if (options.Suggestions.HasValue)
                field.HasSuggestions = options.Suggestions.Value;


            field.Aggregation = options.Aggregation;
            field.GroupByArrayBehavior = options.GroupByArrayBehavior;

            return field;
        }

        public List<IndexField> ToIndexFields(Reference<int> lastUsedId)
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
                    Spatial = new AutoSpatialOptions(Spatial),
                    Vector = null,
                    Id = Id
                });

                return fields;
            }

            if (Vector != null)
            {
                var vector = new AutoVectorOptions(Vector);
                fields.Add(new IndexField
                {
                    Indexing = FieldIndexing.Default,
                    Name =  Name,
                    Storage = Storage,
                    HasSuggestions = HasSuggestions,
                    Spatial = null,
                    Vector = vector,
                    Id = Id
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
                HasSuggestions = HasSuggestions,
                Id = Id
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
                    TermVector = hasHighlighting ? FieldTermVector.WithPositionsAndOffsets : FieldTermVector.No,
                    Id = ++lastUsedId.Value
                });
            }

            if (Indexing.HasFlag(AutoFieldIndexing.Exact))
            {
                fields.Add(new IndexField
                {
                    Indexing = FieldIndexing.Exact,
                    Name = GetExactAutoIndexFieldName(Name),
                    OriginalName = originalName,
                    Storage = Storage,
                    Id = ++lastUsedId.Value
                });
            }

            return fields;
        }

        private bool Equals(AutoIndexField other)
        {
            return string.Equals(Name, other.Name, StringComparison.Ordinal)
                   && Id == other.Id
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
                hashCode = (hashCode * 397) ^ Id;
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

        public static string GetVectorAutoIndexFieldName(string name, VectorOptions vectorOptions)
        {
            var innerMethod = (vectorOptions.SourceEmbeddingType, vectorOptions.DestinationEmbeddingType) switch
            {
                (EmbeddingType.Text, EmbeddingType.Single) => "text",
                (EmbeddingType.Text, EmbeddingType.Int8) => "text_i8",
                (EmbeddingType.Text, EmbeddingType.Binary) => "text_i1",
                (EmbeddingType.Single, EmbeddingType.Single) => string.Empty,
                (EmbeddingType.Single, EmbeddingType.Int8) => "f32_i8",
                (EmbeddingType.Single, EmbeddingType.Binary) => "f32_i1",
                (EmbeddingType.Int8, EmbeddingType.Int8) => "i8",
                (EmbeddingType.Binary, EmbeddingType.Binary) => "i1",
                _ => throw new NotSupportedException($"Unsupported embedding type: ({vectorOptions.SourceEmbeddingType} => {vectorOptions.DestinationEmbeddingType})"),
            };
            
            var inner = innerMethod == string.Empty ? name : $"embedding.{innerMethod}({name})";
            return $"vector.search({inner})";
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
