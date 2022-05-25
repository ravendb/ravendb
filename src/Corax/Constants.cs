using System;
using Sparrow.Server;
using Voron;

namespace Corax
{
    public static class Constants
    {
        public const string NullValue = "null_value";
        public const string EmptyValue = "empty_value";
        public static readonly Slice NullValueSlice, EmptyValueSlice;

        static Constants()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, NullValue, ByteStringType.Immutable, out NullValueSlice);
                Slice.From(ctx, EmptyValue, ByteStringType.Immutable, out EmptyValueSlice);
            }
        }

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
            public static readonly Slice PostingListsSlice, EntriesContainerSlice, FieldsSlice, NumberOfEntriesSlice, SuggestionsFieldsSlice;
            public const int IntKnownFieldMask = unchecked((int)0x80000000);
            public const short ShortKnownFieldMask = unchecked((short)0x8000);
            public const short ByteKnownFieldMask = unchecked((byte)0x80);

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
