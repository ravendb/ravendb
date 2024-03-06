using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Facets
{
    /// <summary>
    /// Optional configuration for facet query.
    /// </summary>
    public sealed class FacetOptions
    {
        internal static readonly FacetOptions Default = new FacetOptions();

        /// <inheritdoc cref="FacetOptions"/>
        public FacetOptions()
        {
            PageSize = int.MaxValue;
        }

        /// <summary>
        /// Indicates how terms should be sorted.
        /// </summary>
        /// <value>FacetTermSortMode.ValueAsc by default.</value>
        public FacetTermSortMode TermSortMode { get; set; }

        /// <summary>
        /// Indicates if remaining terms should be included in results.
        /// </summary>
        public bool IncludeRemainingTerms { get; set; }

        /// <summary>
        /// The position from which to send items (how many to skip).
        /// </summary>
        public int Start { get; set; }

        /// <summary>
        /// Number of items to return. Default: <b><see cref="int.MaxValue"/></b>.
        /// </summary>
        public int PageSize { get; set; }

        internal static FacetOptions Create(BlittableJsonReaderObject json)
        {
            var result = new FacetOptions();

            if (json.TryGet(nameof(result.TermSortMode), out string termSortMode))
                result.TermSortMode = (FacetTermSortMode)Enum.Parse(typeof(FacetTermSortMode), termSortMode, ignoreCase: true);

            if (json.TryGet(nameof(result.IncludeRemainingTerms), out bool includeRemainingTerms))
                result.IncludeRemainingTerms = includeRemainingTerms;

            if (json.TryGet(nameof(result.Start), out int start))
                result.Start = start;

            if (json.TryGet(nameof(result.PageSize), out int pageSize))
                result.PageSize = pageSize;

            return result;
        }
    }
}
