using System;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.Lookups;

namespace Corax
{
    public static class Constants
    {
        public const string BeforeAllKeys = "BeforeAllKeys-a8e5f221-613e-4eae-9962-2689e7c44506";
        public const string AfterAllKeys = "AfterAllKeys-3622a0bb-1cf4-4200-b830-5e937d57ac99";
        
        public static readonly string NullValue = Encodings.Utf8.GetString(NullValueSpan);
        public static readonly ReadOnlyMemory<char> NullValueCharSpan = new(Constants.NullValue.ToCharArray());
        public static ReadOnlySpan<byte> NullValueSpan => new[] { (byte)0 };
        
        public const string EmptyString = "\u0003";
        public static readonly ReadOnlyMemory<char> EmptyStringCharSpan = new(Constants.EmptyString.ToCharArray());
        public static ReadOnlySpan<byte> EmptyStringByteSpan => "\u0003"u8;

        private const string NonExistingValue = "\u0001";
        
        public static ReadOnlySpan<byte> PhraseQuerySuffix => "__PQ"u8; 
        public const string PhraseQuerySuffixAsStr = "__PQ"; 
        
        public const string IndexMetadata = "@index_metadata";
        public const string IndexTimeFields = "@index_time_fields";
        public const string DocumentBoost = "@document_boost";
        public const string ProjectionNullValue = "NULL_VALUE";
        public const string ProjectionEmptyString = "EMPTY_STRING";
        public const string JsonValue = "JSON_VALUE";

        public static readonly Slice NullValueSlice, ProjectionNullValueSlice, EmptyStringSlice, IndexMetadataSlice, DocumentBoostSlice, IndexTimeFieldsSlice, NonExistingValueSlice, ProjectionEmptyStringSlice;

        static Constants()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, ProjectionNullValue, ByteStringType.Immutable, out ProjectionNullValueSlice);
                Slice.From(ctx, ProjectionEmptyString, ByteStringType.Immutable, out ProjectionEmptyStringSlice);
                Slice.From(ctx, NullValue, ByteStringType.Immutable, out NullValueSlice);
                Slice.From(ctx, EmptyString, ByteStringType.Immutable, out EmptyStringSlice);
                Slice.From(ctx, IndexMetadata, ByteStringType.Immutable, out IndexMetadataSlice);
                Slice.From(ctx, DocumentBoost, ByteStringType.Immutable, out DocumentBoostSlice);
                Slice.From(ctx, IndexTimeFields, ByteStringType.Immutable, out IndexTimeFieldsSlice);
                Slice.From(ctx, NonExistingValue, ByteStringType.Immutable, out NonExistingValueSlice);
            }
        }
        
        public static class Terms
        {
            public const int MaxLength = 512;
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

        public static class IndexedField
        {
            public const long Invalid = -1L;
        }
        
        public static class IndexWriter
        {
            public static ReadOnlySpan<byte> DoubleTreeSuffix => "-D"u8;

            public static ReadOnlySpan<byte> LongTreeSuffix => "-L"u8;

            public static readonly Slice LargePostingListsSetSlice, PostingListsSlice,  EntryIdToLocationSlice, LastEntryIdSlice, 
                StoredFieldsSlice, EntriesTermsContainerSlice, FieldsSlice, NumberOfEntriesSlice, EntriesToSpatialSlice, EntriesToTermsSlice,
                DynamicFieldsAnalyzersSlice, NumberOfTermsInIndex, MultipleTermsInField, NullPostingLists, NonExistingPostingLists;            
            
            public const int DynamicField = -2;

            public const int PrimaryKeyFieldId = 0;

            public const string SuggestionsTreePrefix = "__Suggestion_";

            public const int TermFrequencyShift = 8;

            public const int FrequencyTermFreeSpace = 0b1111_1111;
            public const int MaxSizeOfTermVectorList = int.MaxValue >> 1;

            static IndexWriter()
            {
                using (StorageEnvironment.GetStaticContext(out var ctx))
                {
                    Slice.From(ctx, "Fields", ByteStringType.Immutable, out FieldsSlice);
                    Slice.From(ctx, "PostingLists", ByteStringType.Immutable, out PostingListsSlice);
                    Slice.From(ctx, "LargePostingListsSet", ByteStringType.Immutable, out LargePostingListsSetSlice);
                    Slice.From(ctx, "StoredFields", ByteStringType.Immutable, out StoredFieldsSlice);
                    Slice.From(ctx, "EntriesTerms", ByteStringType.Immutable, out EntriesTermsContainerSlice);
                    Slice.From(ctx, "EntryIdToLocation", ByteStringType.Immutable, out EntryIdToLocationSlice);
                    Slice.From(ctx, "NumberOfEntries", ByteStringType.Immutable, out NumberOfEntriesSlice);
                    Slice.From(ctx, "LastEntryId", ByteStringType.Immutable, out LastEntryIdSlice);
                    Slice.From(ctx, "EntriesToTerms", ByteStringType.Immutable, out EntriesToTermsSlice);
                    Slice.From(ctx, "EntriesToSpatial", ByteStringType.Immutable, out EntriesToSpatialSlice);
                    Slice.From(ctx, "DynamicFieldsAnalyzers", ByteStringType.Immutable, out DynamicFieldsAnalyzersSlice);
                    Slice.From(ctx, "NumberOfTermsInIndex", ByteStringType.Immutable, out NumberOfTermsInIndex);
                    Slice.From(ctx, "MultipleTermsInField", ByteStringType.Immutable, out MultipleTermsInField);
                    Slice.From(ctx, "NullPostingLists", ByteStringType.Immutable, out NullPostingLists);
                    Slice.From(ctx, "NonExistingPostingLists", ByteStringType.Immutable, out NonExistingPostingLists);
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
            public enum SearchMatchOptions
            {
                TermMatch = 0,
                StartsWith = 1,
                EndsWith = 1 << 1,
                Exists = 1 << 2,
                Contains = StartsWith | EndsWith
            }

            public enum Operator
            {
                Or,
                And
            }
        }

        public static class Analyzers
        {
            public const int DefaultBufferForAnalyzers = 4 * Sparrow.Global.Constants.Size.Kilobyte;
        }

        public static class Suggestions
        {
            public const int DefaultNGramSize = 4;

            public const float DefaultAccuracy = 0.7f;
        }
        
        /// <summary>
        /// Constants used for QueryPlan.
        /// </summary>
        internal static class QueryInspectionNode
        {
            internal const string FieldName = nameof(FieldName);
            internal const string IsBoosting = nameof(IsBoosting);
            
            /// <summary>
            /// Term used in primitive. For primitives that uses multiple term we will output string as 'term1, term2,...'.
            /// </summary>
            internal const string Term = nameof(Term);
            
            /// <summary>
            /// EndsWith term
            /// </summary>
            internal const string Suffix = nameof(Suffix);
            
            /// <summary>
            /// StartsWith term
            /// </summary>
            internal const string Prefix = nameof(Prefix);
            
            /// <summary>
            /// Values of range queries.
            /// </summary>
            internal const string LowValue = nameof(LowValue);
            internal const string HighValue = nameof(HighValue);

            /// <summary>
            /// Indicates if Low/High value is inclusive or exclusive.
            /// </summary>
            internal const string LowOption = nameof(LowOption);
            internal const string HighOption = nameof(HighOption);
            
            /// <summary>
            /// Indicates direction of crawling on the tree. 
            /// </summary>
            internal const string IteratorDirection = nameof(IteratorDirection);
            
            /// <summary>
            /// Confidence of count. Look into IQueryMatch.cs for details.
            /// </summary>
            internal const string CountConfidence = nameof(CountConfidence);
            
            /// <summary>
            /// Count of documents from primitive.
            /// </summary>
            internal const string Count = nameof(Count);
            
            /// <summary>
            /// Boost factor for boost(InnerQuery, BoostFactor)
            /// </summary>
            internal const string BoostFactor = nameof(BoostFactor);
            
            /// <summary>
            /// Used for MultiUnaryMatch. This primitive can have multiple comparers inside, so we will output the settings as a string.
            /// </summary>
            internal const string Comparer = nameof(Comparer);
            
            /// <summary>
            /// Used for UnaryMatch to determinate which operation is executed.
            /// </summary>
            internal const string Operation = nameof(Operation);
            
            //Sorting:
            /// <summary>
            /// Indicates central point for distance sorting
            /// </summary>
            internal const string Point = nameof(Point);
            
            /// <summary>
            /// Round property for spatial order by distance()
            /// </summary>
            internal const string Round = nameof(Round);
            /// <summary>
            /// Kilometers or miles.
            /// </summary>
            internal const string Units = nameof(Units);
            
            /// <summary>
            /// Seed for order by random()
            /// </summary>
            internal const string RandomSeed = nameof(RandomSeed);

            /// <summary>
            /// Indicates if order is ascending.
            /// </summary>
            internal const string Ascending = nameof(Ascending);
            
            /// <summary>
            /// Indicates type of sorted field. (eg, long, alphanumerical etc)
            /// </summary>
            internal const string FieldType = nameof(FieldType);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static string IterationDirectionName<TLookupIterator>(in TLookupIterator lookupIterator)
                where TLookupIterator : struct, ILookupIterator
            {
                return lookupIterator.IsForward ? "Forward" : "Backward";
            }
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static string IterationDirectionName<TLookupIterator>()
                where TLookupIterator : struct, ILookupIterator
            {
                return default(TLookupIterator).IsForward ? "Forward" : "Backward";
            }
        }
    }
}
