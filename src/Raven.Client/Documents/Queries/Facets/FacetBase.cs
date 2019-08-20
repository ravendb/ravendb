using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Raven.Client.Documents.Session.Tokens;
using Sparrow.Json;

namespace Raven.Client.Documents.Queries.Facets
{
    public abstract class FacetBase
    {
        [JsonIgnore]
        private string _displayFieldName;

        protected FacetBase()
        {
            Aggregations = new Dictionary<FacetAggregation, HashSet<string>>();
        }

        public FacetOptions Options { get; set; }

        public Dictionary<FacetAggregation, HashSet<string>> Aggregations { get; set; }

        /// <summary>
        /// Displayed field name in facet results
        /// </summary>
        public string DisplayFieldName
        {
            get => _displayFieldName;
            set => _displayFieldName = value;
        }

        internal abstract FacetToken ToFacetToken(Func<object, string> addQueryParameter);

        internal static void Fill(FacetBase facet, BlittableJsonReaderObject json)
        {
            if (facet == null) 
                throw new ArgumentNullException(nameof(facet));
            if (json == null) 
                throw new ArgumentNullException(nameof(json));

            if (json.TryGet(nameof(facet.DisplayFieldName), out string displayFieldName))
                facet.DisplayFieldName = displayFieldName;

            if (json.TryGet(nameof(facet.Options), out BlittableJsonReaderObject options) && options != null)
                facet.Options = FacetOptions.Create(options);

            if (json.TryGet(nameof(facet.Aggregations), out BlittableJsonReaderObject aggregations) && aggregations != null)
            {
                foreach (string propertyName in aggregations.GetPropertyNames())
                {
                    var aggregation = (FacetAggregation)Enum.Parse(typeof(FacetAggregation), propertyName, ignoreCase: true);

                    var value = aggregations[propertyName];
                    if (value is BlittableJsonReaderArray array)
                    {
                        var fields = facet.Aggregations[aggregation] = new HashSet<string>();

                        foreach (var field in array)
                            fields.Add(field.ToString());

                        continue;
                    }

                    facet.Aggregations[aggregation] = new HashSet<string>
                    {
                        value.ToString()
                    };
                }
            }
        }
    }
}
