using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;

namespace Raven.Server.Documents.Indexes
{
    public class IndexField
    {
        private static readonly Regex ReplaceInvalidCharacterForFields = new Regex(@"[^\w_]", RegexOptions.Compiled);

        public string Name { get; set; }

        public string Analyzer { get; set; }

        public SortOptions? SortOption { get; set; }

        public bool Highlighted { get; set; }

        public FieldMapReduceOperation MapReduceOperation { get; set; }

        public FieldStorage Storage { get; set; }

        public FieldIndexing Indexing { get; set; }

        public static string ReplaceInvalidCharactersInFieldName(string field)
        {
            //TODO: This is probably expensive, we can do better
            return ReplaceInvalidCharacterForFields.Replace(field, "_");
        }

        public IndexField()
        {
            Indexing = FieldIndexing.Default;
            Storage = FieldStorage.No;
        }

        public static IndexField Create(string name, IndexFieldOptions options)
        {
            var field = new IndexField();
            field.Name = name;
            field.Analyzer = options.Analyzer;

            if (options.Indexing.HasValue)
                field.Indexing = options.Indexing.Value;

            field.SortOption = options.Sort;

            if (options.Storage.HasValue)
                field.Storage = options.Storage.Value;

            // options.TermVector // TODO [ppekrol]
            // options.Suggestions // TODO [ppekrol]
            // options.Spatial // TODO [ppekrol]

            return field;
        }

        protected bool Equals(IndexField other)
        {
            return string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase)
                && string.Equals(Analyzer, other.Analyzer, StringComparison.OrdinalIgnoreCase)
                && SortOption == other.SortOption
                && Highlighted == other.Highlighted
                && MapReduceOperation == other.MapReduceOperation
                && Storage == other.Storage
                && Indexing == other.Indexing;
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
                hashCode = (hashCode * 397) ^ SortOption.GetHashCode();
                hashCode = (hashCode * 397) ^ Highlighted.GetHashCode();
                hashCode = (hashCode * 397) ^ (int)MapReduceOperation;
                hashCode = (hashCode * 397) ^ (int)Storage;
                hashCode = (hashCode * 397) ^ (int)Indexing;
                return hashCode;
            }
        }
    }
}