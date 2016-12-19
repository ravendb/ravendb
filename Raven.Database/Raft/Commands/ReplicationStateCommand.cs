using System.Threading.Tasks;
using Rachis.Commands;
using Raven.Database.Raft.Dto;

namespace Raven.Database.Raft.Commands
{
    public class ReplicationStateCommand : Command
    {
        /// <summary>
        /// A map between database name to the last modified time and the transactional id.
        /// </summary>
        public ReplicationState DatabaseToLastModified { get; set; }

        public static ReplicationStateCommand Create
            (ReplicationState databaseToLastModified)
        {
            return new ReplicationStateCommand
            {
                DatabaseToLastModified = databaseToLastModified,
                Completion = new TaskCompletionSource<object>()
            };
        }
    }
}
