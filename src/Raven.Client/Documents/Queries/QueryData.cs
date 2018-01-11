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
        public class CustomFunction
        {
            public string FromAlias { get; set; }
        }
        
        public string[] Fileds { get; set; }

        public IEnumerable<string> Projections { get; set; }

        public string FromAlias { get; set; }

        public DeclareToken DeclareToken { get; set; }

        public List<LoadToken> LoadTokens { get; set; }

        public bool IsCustomFunction { get; set; }

        public QueryData(string[] fileds, IEnumerable<string> projections, string fromAlias, DeclareToken declareToken, List<LoadToken> loadTokens, bool isCustomFunction = false)
        {
            Fileds = fileds;
            Projections = projections;
            FromAlias = fromAlias;
            DeclareToken = declareToken;
            LoadTokens = loadTokens;
            IsCustomFunction = isCustomFunction;
        }

        public QueryData(string[] fileds, IEnumerable<string> projections, CustomFunction customFunction = null, DeclareToken declareToken = null, List<LoadToken> loadTokens = null)
        {
            Fileds = fileds;
            Projections = projections;
            LoadTokens = loadTokens;
            DeclareToken = declareToken;
            FromAlias = customFunction?.FromAlias;
            IsCustomFunction = customFunction != null;
        }
        
    }
}
