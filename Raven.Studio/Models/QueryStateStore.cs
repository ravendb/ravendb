using System.Collections.Generic;
using Raven.Studio.Features.Query;

namespace Raven.Studio.Models
{
    public class QueryStateStore
    {
        private Dictionary<string,QueryState> _indexQueryStates = new Dictionary<string, QueryState>();
    }
}
