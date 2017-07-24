// -----------------------------------------------------------------------
//  <copyright file="FacetQuery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Documents.Queries.Facets
{
    public class FacetQuery : FacetQuery<Dictionary<string, object>>
    {
        public static FacetQuery Create(IndexQueryBase<Dictionary<string, object>> query, string facetSetupDoc, List<Facet> facets, int start, int? pageSize, DocumentConventions conventions)
        {
            var result = new FacetQuery
            {
                CutoffEtag = query.CutoffEtag,
                Query = query.Query,
                QueryParameters = query.QueryParameters,
                WaitForNonStaleResults = query.WaitForNonStaleResults,
                WaitForNonStaleResultsAsOfNow = query.WaitForNonStaleResultsAsOfNow,
                WaitForNonStaleResultsTimeout = query.WaitForNonStaleResultsTimeout,
                Start = start,
                FacetSetupDoc = facetSetupDoc,
                Facets = facets
            };

            if (pageSize.HasValue)
                result.PageSize = pageSize.Value;

            return result;
        }

        public uint GetQueryHash()
        {
            unchecked
            {
                var hashCode = Query?.GetHashCode() ?? 0;
                if (FacetSetupDoc != null)
                    hashCode = (hashCode * 397) ^ FacetSetupDoc.GetHashCode();

                hashCode = (hashCode * 397) ^ WaitForNonStaleResults.GetHashCode();
                hashCode = (hashCode * 397) ^ WaitForNonStaleResultsAsOfNow.GetHashCode();
                hashCode = (hashCode * 397) ^ (WaitForNonStaleResultsTimeout?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (CutoffEtag?.GetHashCode() ?? 0);

                hashCode = (hashCode * 397) ^ Start;
                hashCode = (hashCode * 397) ^ PageSize;

                hashCode = (hashCode * 397) ^ QueryHashHelper.HashCode(Facets);

                if (QueryParameters != null)
                    hashCode = (hashCode * 397) ^ QueryHashHelper.HashCode(QueryParameters);

                return (uint)hashCode;
            }
        }
    }

    public abstract class FacetQuery<T> : IndexQueryBase<T>
    {
        /// <summary>
        /// Id of a facet setup document that can be found in database containing facets (mutually exclusive with Facets).
        /// </summary>
        public string FacetSetupDoc { get; set; }

        /// <summary>
        /// List of facets (mutually exclusive with FacetSetupDoc).
        /// </summary>
        public IReadOnlyList<Facet> Facets { get; set; }
    }
}
