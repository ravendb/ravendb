using System.Collections;
using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.Replication
{
    public class ReplicationHubAccessResult : IEnumerable<DetailedReplicationHubAccess>
    {
        public List<DetailedReplicationHubAccess> Results = new List<DetailedReplicationHubAccess>();
        
        public IEnumerator<DetailedReplicationHubAccess> GetEnumerator()
        {
            return Results.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}