using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Facets
{
    public class FacetSetup
    {
        /// <summary>
        /// Id of a facet setup document.
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// List of facets.
        /// </summary>
        public List<Facet> Facets { get; set; }

        /// <summary>
        /// List of range facets.
        /// </summary>
        public List<RangeFacet> RangeFacets { get; set; }

        public FacetSetup()
        {
            Facets = new List<Facet>();
            RangeFacets = new List<RangeFacet>();
        }

        internal static FacetSetup Create(string id, BlittableJsonReaderObject json)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));
            if (json == null)
                throw new ArgumentNullException(nameof(json));

            var result = new FacetSetup { Id = id };

            if (json.TryGet(nameof(result.Facets), out BlittableJsonReaderArray array) && array != null)
                result.Facets = CreateFacets(array);

            if (json.TryGet(nameof(result.RangeFacets), out array) && array != null)
                result.RangeFacets = CreateRangeFacets(array);

            return result;
        }

        private static List<RangeFacet> CreateRangeFacets(BlittableJsonReaderArray array)
        {
            var results = new List<RangeFacet>();
            foreach (BlittableJsonReaderObject json in array)
                results.Add(RangeFacet.Create(json));

            return results;
        }

        private static List<Facet> CreateFacets(BlittableJsonReaderArray array)
        {
            var results = new List<Facet>();
            foreach (BlittableJsonReaderObject json in array)
                results.Add(Facet.Create(json));

            return results;
        }
    }
}
