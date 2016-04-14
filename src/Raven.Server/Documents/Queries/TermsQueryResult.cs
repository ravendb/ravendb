using System.Collections.Generic;

namespace Raven.Server.Documents.Queries
{
    public class TermsQueryResult
    {
        public bool NotModified { get; set; }

        public HashSet<string> Terms { get; set; }

        public long ResultEtag { get; set; }

        public string IndexName { get; set; }
    }
}