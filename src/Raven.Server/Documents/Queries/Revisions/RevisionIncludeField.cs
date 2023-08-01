using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries.Revisions
{
    public sealed class RevisionIncludeField
    {
        internal HashSet<string> RevisionsChangeVectorsPaths;
        internal DateTime? RevisionsBeforeDateTime;

        public RevisionIncludeField()
        {
            RevisionsChangeVectorsPaths = new HashSet<string>();
            RevisionsBeforeDateTime = new DateTime?();
        }
        
        public void AddRevision(string path)
        {
            RevisionsChangeVectorsPaths.Add(path);
        }
        
        public void AddRevision(DateTime dateTime)
        {
            RevisionsBeforeDateTime = dateTime;
        }
    }
}
