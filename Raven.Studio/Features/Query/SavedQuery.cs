using System;
using System.Collections.Generic;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Client.Silverlight.MissingFromSilverlight;
using Raven.Abstractions.Extensions;

namespace Raven.Studio.Features.Query
{
    public class SavedQuery
    {
        public SavedQuery(string indexName, string query)
        {
            IndexName = indexName;
            Query = query;
            SortOptions = new List<string>();
        }

        public string IndexName { get; private set; }

        public string Query { get; private set; }

        public IList<string> SortOptions { get; private set; }

        public bool IsSpatialQuery { get; set; }
        public double? Latitude { get; set; }
        public double? Longitude { get; set; }
        public double? Radius { get; set; }

        public string Hashcode
        {
            get { return QueryState.CreateQueryStateHash(IndexName, Query, IsSpatialQuery, Latitude, Longitude, Radius); }
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
