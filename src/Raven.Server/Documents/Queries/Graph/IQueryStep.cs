using System.Collections.Generic;
using System.Threading.Tasks;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public interface IQueryStep
    {
        Task<IEnumerable<Match>> Execute(Dictionary<IQueryStep,IEnumerable<Match>> matches);
    }
}
