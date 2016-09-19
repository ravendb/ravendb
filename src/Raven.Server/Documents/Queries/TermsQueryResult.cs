using System.Collections.Generic;

namespace Raven.Server.Documents.Queries
{
    public class TermsQueryResult
    {
        public static readonly TermsQueryResult NotModifiedResult = new TermsQueryResult { NotModified = true };

        public bool NotModified { get; private set; }

        public HashSet<string> Terms { get; set; }

        public long ResultEtag { get; set; }

        public string IndexName { get; set; }
    }
}