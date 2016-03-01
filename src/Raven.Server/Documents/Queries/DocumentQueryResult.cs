using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Server.Documents.Queries
{
    public class DocumentQueryResult : QueryResult<Document>
    {
        public HashSet<string> IdsToInclude { get; set; }
    }
}