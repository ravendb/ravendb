using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Queries.Graph
{
    public interface IGraphQueryStep
    {
        ValueTask Initialize();

        HashSet<string> GetAllAliases();

        string GetOutputAlias();

        bool GetNext(out GraphQueryRunner.Match match);

        List<GraphQueryRunner.Match> GetById(string id);

        void Analyze(GraphQueryRunner.Match match,
            Action<string, object> addNode, 
            Action<object, string> addEdge);

        IGraphQueryStep Clone();

        ISingleGraphStep GetSingleGraphStepExecution();
    }

    public interface ISingleGraphStep
    {
        ValueTask Initialize();

        bool GetAndClearResults(List<GraphQueryRunner.Match> matches);

        void Run(GraphQueryRunner.Match src);
}
}
