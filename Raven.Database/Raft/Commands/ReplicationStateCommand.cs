using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rachis.Commands;

namespace Raven.Database.Raft.Commands
{
    public class ReplicationStateCommand : Command
    {
        /// <summary>
        /// A map between database name to the last modified time and the transactional id.
        /// </summary>
        public Dictionary<string, Tuple<DateTime,string>> DatabaseToLastModified { get; set; }

        public static ReplicationStateCommand Create
            (Dictionary<string, Tuple<DateTime, string>> databaseToLastModified)
        {
            return new ReplicationStateCommand()
            {
                DatabaseToLastModified = databaseToLastModified,
                Completion = new TaskCompletionSource<object>()
            };
        }
    }
}
