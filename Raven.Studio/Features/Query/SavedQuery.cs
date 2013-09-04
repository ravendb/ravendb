using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Studio.Features.Query
{
    public class SavedQuery
    {
        [JsonConstructor]
        public SavedQuery(string indexName, string query)
        {
            IndexName = indexName;
            Query = query;
            SortOptions = new List<string>();
        }

		public SavedQuery(QueryState state)
		{
			if (state == null)
				return;

			IndexName = state.IndexName;
			Query = state.Query;
		    SortOptions = state.SortOptions;
			IsSpatialQuery = state.IsSpatialQuery;
			SpatialFieldName = state.SpatialFieldName;
			Latitude = state.Latitude;
			Longitude = state.Longitude;
			Radius = state.Radius;
			DefaultOperator = state.DefaultOperator;
			ShowFields = state.ShowFields;
			ShowEntries = state.ShowEntries;
			UseTransformer = state.UseTransformer;
			SkipTransform = state.SkipTransform;
			Transformer = state.Transformer;
	    }

	    public string IndexName { get; private set; }

        public string Query { get; private set; }

        public IList<string> SortOptions { get; private set; }

        public bool IsSpatialQuery { get; set; }
		public string SpatialFieldName { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public double? Radius { get; set; }
		public QueryOperator DefaultOperator { get; set; }
		public bool ShowFields { get; set; }
		public bool ShowEntries { get; set; }
		public bool UseTransformer { get; set; }
		public bool SkipTransform { get; set; }
		public string Transformer { get; set; }

        public string Hashcode
        {
            get { return QueryState.CreateQueryStateHash(new QueryState(this)); }
        }

        public bool IsPinned { get; set; }

        public void UpdateFrom(QueryState state)
        {
            SortOptions.Clear();
            SortOptions.AddRange(state.SortOptions);

            IsSpatialQuery = state.IsSpatialQuery;
            Latitude = state.Latitude;
            Longitude = state.Longitude;
            Radius = state.Radius;
        }
    }
}