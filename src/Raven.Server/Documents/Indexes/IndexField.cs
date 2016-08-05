using System;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;

namespace Raven.Server.Documents.Indexes
{
    public class IndexField
    {
        public string Name { get; set; }

        public string Analyzer { get; set; }

        public SortOptions? SortOption { get; set; }

        public bool Highlighted { get; set; }

        public FieldMapReduceOperation MapReduceOperation { get; set; }

        public FieldStorage Storage { get; set; }

        public FieldIndexing Indexing { get; set; }

        public FieldTermVector TermVector { get; set; }

        public static string ReplaceInvalidCharactersInFieldName(string field)
        {
            // we allow only \w which is equivalent to [a-zA-Z_0-9]
            const int a = 'a';
            const int z = 'z';
            const int A = 'A';
            const int Z = 'Z';
            const int Zero = '0';
            const int Nine = '9';
            const int Underscore = '_';

            if (string.IsNullOrEmpty(field))
                return field;

            char[] input = null;
            
            for (var i = 0; i < field.Length; i++)
            {
                var ch = field[i];
                if (ch >= a && ch <= z)
                    continue;

                if (ch >= A && ch <= Z)
                    continue;

                if (ch >= Zero && ch <= Nine)
                    continue;

                if (ch == Underscore)
                    continue;

                if (input == null)
                {
                    input = new char[field.Length];
                    field.CopyTo(0, input, 0, field.Length);
                }

                input[i] = '_';
            }

            return input == null ? field : new string(input);
        }

        public IndexField()
        {
            Indexing = FieldIndexing.Default;
            Storage = FieldStorage.No;
        }

        public static IndexField Create(string name, IndexFieldOptions options, IndexFieldOptions allFields)
        {
            var field = new IndexField();
            field.Name = name;
            field.Analyzer = options.Analyzer ?? allFields?.Analyzer;

            if (options.Indexing.HasValue)
                field.Indexing = options.Indexing.Value;
            else if (string.IsNullOrWhiteSpace(field.Analyzer) == false)
                field.Indexing = FieldIndexing.Analyzed;

            if (options.Sort.HasValue)
                field.SortOption = options.Sort.Value;
            else if (allFields?.Sort != null)
                field.SortOption = allFields.Sort.Value;

            if (options.Storage.HasValue)
                field.Storage = options.Storage.Value;
            else if (allFields?.Storage != null)
                field.Storage = allFields.Storage.Value;

            if (options.TermVector.HasValue)
                field.TermVector = options.TermVector.Value;
            else if (allFields?.TermVector != null)
                field.TermVector = allFields.TermVector.Value;

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
                hashCode = (hashCode * 397) ^ SortOption.GetHashCode();
                hashCode = (hashCode * 397) ^ Highlighted.GetHashCode();
                hashCode = (hashCode * 397) ^ (int)MapReduceOperation;
                hashCode = (hashCode * 397) ^ (int)Storage;
                hashCode = (hashCode * 397) ^ (int)Indexing;
                hashCode = (hashCode * 397) ^ (int)TermVector;
                return hashCode;
            }
        }
    }
}