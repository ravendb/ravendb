using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.Queries.AST;

namespace Corax.Queries
{
    public class QueryDefinition
    {
        /// <summary>
        /// This is the means by which the outside world refers to this query
        /// </summary>
        public string Name { get; private set; }

        public Query Query { get; private set; }

        public QueryDefinition(string name, Query query )
        {
            Name = name;
            Query = query;
        }
    }
}
