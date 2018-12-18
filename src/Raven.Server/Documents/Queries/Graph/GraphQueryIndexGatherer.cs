using System;
using System.Collections.Generic;
using System.Text;
using NCrontab.Advanced.Extensions;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Documents.Queries.Graph
{
    public class GraphQueryIndexNamesGatherer: QueryPlanVisitor
    {
        public List<string> Indexes { get; set; } = new List<string>();

        public override void VisitQueryQueryStep(QueryQueryStep qqs)
        {
            var name = qqs.GetIndexName;
            if(name.IsNullOrWhiteSpace() == false)
                Indexes.Add(name);
        }
    }
}
