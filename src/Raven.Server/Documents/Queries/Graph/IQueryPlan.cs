using System.Collections.Generic;
using System.Threading.Tasks;
using static Raven.Server.Documents.Queries.GraphQueryRunner;

namespace Raven.Server.Documents.Queries.Graph
{
    public interface IQueryPlan 
    {
        void BuildQueryPlan();
        void OptimizeQueryPlan();
        ValueTask<IEnumerable<Match>> ExecuteQueryPlan();
    }
}
