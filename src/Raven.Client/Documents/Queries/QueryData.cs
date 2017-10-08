//-----------------------------------------------------------------------
// <copyright file="QueryData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Queries
{
    public class QueryData
    {
        public string[] Fileds { get; set; }

        public IEnumerable<string> Projections { get; set; }

        public string FromAlias { get; set; }

        public DeclareToken DeclareToken { get; set; }

        public List<LoadToken> LoadTokens { get; set; }

        public bool IsCustomFunction { get; set; }

        public QueryData(string[] fileds, IEnumerable<string> projections, string fromAlias = null, DeclareToken declareToken = null, List<LoadToken> loadTokens = null , bool isCustomFunction = false)
        {
            Fileds = fileds;
            Projections = projections;
            FromAlias = fromAlias;
            DeclareToken = declareToken;
            LoadTokens = loadTokens;
            IsCustomFunction = isCustomFunction;
        }
    }
}
