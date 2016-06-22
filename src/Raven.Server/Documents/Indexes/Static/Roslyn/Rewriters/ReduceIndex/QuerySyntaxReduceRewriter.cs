using System;
using Microsoft.CodeAnalysis.CSharp;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public class QuerySyntaxReduceRewriter : ReduceRewriterBase
    {
        private readonly QuerySyntaxResultsVariableNameRetriever _resultsVariableNameRetriever = new QuerySyntaxResultsVariableNameRetriever();

        public QuerySyntaxReduceRewriter()
        {
            Rewriters = new CSharpSyntaxRewriter[]
            {
                _resultsVariableNameRetriever
            };
        }

        public override string ResultsVariableName => _resultsVariableNameRetriever.ResultsVariableName;
    }
}