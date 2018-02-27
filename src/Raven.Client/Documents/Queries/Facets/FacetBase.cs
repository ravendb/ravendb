using System.Collections.Generic;
using Newtonsoft.Json;

namespace Raven.Client.Documents.Queries.Facets
{
    public abstract class FacetBase
    {
        [JsonIgnore]
        private string _displayFieldName;

        protected FacetBase()
        {
            Aggregations = new Dictionary<FacetAggregation, string>();
        }

        public FacetOptions Options { get; set; }

        public Dictionary<FacetAggregation, string> Aggregations { get; set; }

        public Parameters FacetParameters { get; set; }

        /// <summary>
        /// Displayed field name in facet results
        /// </summary>
        public string DisplayFieldName
        {
            get => _displayFieldName;
            set => _displayFieldName = value;
        }

        internal abstract Facet AsFacet();

        internal abstract RangeFacet AsRangeFacet();
    }
}
