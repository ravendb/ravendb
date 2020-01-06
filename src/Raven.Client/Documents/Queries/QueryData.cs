//-----------------------------------------------------------------------
// <copyright file="QueryData.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Queries
{
    public class QueryData
    {
        public string[] Fields { get; set; }

        public IEnumerable<string> Projections { get; set; }

        public string FromAlias { get; set; }

        public IEnumerable<DeclareToken> DeclareTokens { get; set; }

        public List<LoadToken> LoadTokens { get; set; }

        public bool IsCustomFunction { get; set; }

        public bool IsMapReduce { get; set; }

        internal bool IsProjectInto { get; set; }

        public QueryData(string[] fields, IEnumerable<string> projections, string fromAlias = null, IEnumerable<DeclareToken> declareTokens = null, List<LoadToken> loadTokens = null, bool isCustomFunction = false)
        {
            Fields = fields;
            Projections = projections;
            FromAlias = fromAlias;
            DeclareTokens = declareTokens;
            LoadTokens = loadTokens;
            IsCustomFunction = isCustomFunction;
        }

        public static QueryData CustomFunction(string alias, string func)
        {
            return new QueryData(new[] {func}, Array.Empty<string>(), alias, isCustomFunction: true);
        }
    }
}
