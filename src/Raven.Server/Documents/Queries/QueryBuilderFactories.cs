using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Server.Documents.Indexes.Static.Spatial;

namespace Raven.Server.Documents.Queries
{
    public class QueryBuilderFactories
    {
        public Func<string, SpatialField> GetSpatialFieldFactory { get; set; }
        public Func<string, Regex> GetRegexFactory { get; set; }
    }
}
