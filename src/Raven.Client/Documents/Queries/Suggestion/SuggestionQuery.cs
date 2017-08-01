//-----------------------------------------------------------------------
// <copyright file="SuggestionQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Suggestion
{
    /// <summary>
    /// 
    /// </summary>
    public class SuggestionQuery
    {
        public static float DefaultAccuracy = 0.5f;

        public static int DefaultMaxSuggestions = 15;

        public static StringDistanceTypes DefaultDistance = StringDistanceTypes.Levenshtein;

        /// <summary>
        /// Create a new instance of <seealso cref="SuggestionQuery"/>
        /// </summary>
        public SuggestionQuery()
        {
            MaxSuggestions = DefaultMaxSuggestions;
            Popularity = true;
        }

        public string IndexName { get; set; }

        /// <summary>
        /// Term is what the user likely entered, and will used as the basis of the suggestions.
        /// </summary>
        public string Term { get; set; }

        /// <summary>
        /// Field to be used in conjunction with the index.
        /// </summary>
        public string Field { get; set; }

        /// <summary>
        /// Maximum number of suggestions to return.
        /// <para>Value:</para>
        /// <para>Default value is 15.</para>
        /// </summary>
        /// <value>Default value is 15.</value>
        public int MaxSuggestions { get; set; }

        /// <summary>
        /// String distance algorithm to use. If <c>null</c> then default algorithm is used (Levenshtein).
        /// </summary>
        public StringDistanceTypes? Distance { get; set; }

        /// <summary>
        /// Suggestion accuracy. If <c>null</c> then default accuracy is used (0.5f).
        /// </summary>
        public float? Accuracy { get; set; }

        /// <summary>
        /// Whether to return the terms in order of popularity
        /// </summary>
        public bool Popularity { get; set; }

        public ulong GetQueryHash(JsonOperationContext ctx)
        {
            using (var hasher = new QueryHashCalculator(ctx))
            {
                hasher.Write(Popularity);
                hasher.Write(Accuracy);
                hasher.Write((int?)Distance);
                hasher.Write(MaxSuggestions);
                hasher.Write(Field);
                hasher.Write(Term);
                hasher.Write(IndexName);
                return hasher.GetHash();
            }
        }
    }
}