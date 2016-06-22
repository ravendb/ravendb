using Microsoft.CodeAnalysis.CSharp;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public class MethodSyntaxReduceRewriter : ReduceRewriterBase
    {
        private readonly MethodSyntaxResultsVariableNameRetriever _resultsVariableNameRetriever = new MethodSyntaxResultsVariableNameRetriever();

        public MethodSyntaxReduceRewriter()
        {
            Rewriters = new CSharpSyntaxRewriter[]
            {
                _resultsVariableNameRetriever
            };
        }

        public override string ResultsVariableName => _resultsVariableNameRetriever.ResultsVariableName;
    }
}