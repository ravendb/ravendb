using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries.Revisions
{
    public class RevisionIncludeField
    {
        public HashSet<string> Revisions;

        public void AddRevision(string field)
        {
            Revisions ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            Revisions.Add(field);
        }
    }
}
