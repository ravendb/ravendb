using System;
using Sparrow.Server;
using Voron;

namespace Corax
{
    public static class Constants
    {
        public static class Boosting
        {
            public static float ScoreEpsilon = 1e-8F;
        }

        public static class IndexSearcher
        {
            public const int InvalidId = -1;
            public const int TakeAll = -1;
            public const int NonAnalyzer = -1;
        }

        public static class IndexWriter
        {
            public enum FieldIndexing
            {
                /// <summary>
                /// Do not index the field value. This field can thus not be searched, but one can still access its contents provided it is stored.
                /// </summary>
                No,

                /// <summary>
                /// Index the tokens produced by running the field's value through an Analyzer. This is useful for common text.
                /// </summary>
                Search,

                /// <summary>
                /// Index the field's value without using an Analyzer, so it can be searched.  As no analyzer is used the 
                /// value will be stored as a single term. This is useful for unique Ids like product numbers.
                /// </summary>
                Exact,

                /// <summary>
                /// Index this field using the default internal analyzer: LowerCaseKeywordAnalyzer
                /// </summary>
                Default
            }
            
            
            public static readonly Slice PostingListsSlice, EntriesContainerSlice, FieldsSlice, NumberOfEntriesSlice, SuggestionsFieldsSlice;

            static IndexWriter()
            {
                using (StorageEnvironment.GetStaticContext(out var ctx))
                {
                    Slice.From(ctx, "Fields", ByteStringType.Immutable, out FieldsSlice);
                    Slice.From(ctx, "PostingLists", ByteStringType.Immutable, out PostingListsSlice);
                    Slice.From(ctx, "Entries", ByteStringType.Immutable, out EntriesContainerSlice);
                    Slice.From(ctx, "NumberOfEntries", ByteStringType.Immutable, out NumberOfEntriesSlice);
                    Slice.From(ctx, "SuggestionFields", ByteStringType.Immutable, out SuggestionsFieldsSlice);
                }
            }
        }

        public static class StorageMask
        {
            public const long ContainerType = ~0b11;
        }
        
        public static class Search
        {
            public const byte Wildcard = (byte)'*';

            [Flags]
            internal enum SearchMatchOptions
            {
                TermMatch = 0,
                StartsWith = 1,
                EndsWith = 2,
                Contains = 4
            }

            public enum Operator
            {
                Or,
                And
            }
        }

        public static class Suggestions
        {
            public const int DefaultNGramSize = 4;

            public const float DefaultAccuracy = 0.7f;

            public enum Algorithm
            {
                NGram,
                JaroWinkler,
                Levenshtein 
            }
        }
        
    }
}
