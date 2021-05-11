using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries.Revisions
{
    public class RevisionIncludeField
    {
        public RevisionIncludeField()
        {
            Revisions = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
        }

        public readonly Dictionary<string, HashSet<string>> Revisions;
        
        public void AddRevision(string revision, string sourcePath = null)
        {
            var key = sourcePath ?? string.Empty;
            if (Revisions.TryGetValue(key, out var hashSet) == false)
            {
                hashSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                Revisions.Add(key, hashSet);
            }
            hashSet.Add(revision);
        }
    }
}
