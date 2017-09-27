//-----------------------------------------------------------------------
// <copyright file="QueryData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace Raven.Client.Documents.Queries
{
    public class QueryData
    {
        public string[] Fileds { get; set; }

        public string[] Projections { get; set; }

        public string FromAlias { get; set; }

        public QueryData(string[] fileds, string[] projections, string fromAlias)
        {
            Fileds = fileds;
            Projections = projections;
            FromAlias = fromAlias;
        }
    }
}
