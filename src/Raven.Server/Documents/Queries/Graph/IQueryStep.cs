using System.Collections.Generic;
using System.Threading.Tasks;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public interface IQueryStep
    {
        ValueTask<IEnumerable<Match>> Execute(Dictionary<IQueryStep,IEnumerable<Match>> matches);
        IEnumerable<IQueryStep> Dependencies { get; }
    }
}
