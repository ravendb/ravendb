namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public interface IResultsVariableNameRetriever
    {
        string ResultsVariableName { get; }
    }
}