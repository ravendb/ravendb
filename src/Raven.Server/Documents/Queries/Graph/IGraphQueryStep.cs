using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Server.Documents.Queries.Graph
{
    public interface IGraphQueryStep
    {
        ValueTask Initialize();

        HashSet<string> GetAllAliases();

        string GetOuputAlias();

        bool GetNext(out GraphQueryRunner.Match match);

        bool TryGetById(string id, out GraphQueryRunner.Match match);

        void Analyze(GraphQueryRunner.Match match,
            Action<string, object> addNode, 
            Action<object, string> addEdge);
    }
}
