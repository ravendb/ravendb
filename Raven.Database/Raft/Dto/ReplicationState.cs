using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Database.Raft.Dto
{
    public class ReplicationState
    {
        public Dictionary<string, LastModificationTimeAndTransactionalId> DatabasesToLastModification;

        public ReplicationState(Dictionary<string, LastModificationTimeAndTransactionalId> databasesToLastModification)
        {
            DatabasesToLastModification = databasesToLastModification;
        }
    }

    public class LastModificationTimeAndTransactionalId
    {
        public DateTime LastModified { get; set; }
        public string DatabaseId { get; set; }
    }
}
