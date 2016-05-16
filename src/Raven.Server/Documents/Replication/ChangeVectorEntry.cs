using System;

namespace Raven.Server.Documents.Replication
{
    public struct ChangeVectorEntry
    {
        public Guid DbId;
        public long Etag;
    }
}