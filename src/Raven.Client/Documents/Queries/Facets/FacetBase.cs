using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Raven.Client.Documents.Session.Tokens;

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

        /// <summary>
        /// Displayed field name in facet results
        /// </summary>
        public string DisplayFieldName
        {
            get => _displayFieldName;
            set => _displayFieldName = value;
        }

        internal abstract FacetToken ToFacetToken(Func<object, string> addQueryParameter);
    }
}
