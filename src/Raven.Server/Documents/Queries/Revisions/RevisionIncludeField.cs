using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries.Revisions
{
    public class RevisionIncludeField
    {
        public readonly HashSet<string> Revisions;

        public RevisionIncludeField()
        {
            Revisions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public void AddRevision(string field)
        {
            Revisions.Add(field);
        }
    }
}
