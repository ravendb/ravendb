using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.ServerWide.Commands
{
    public class GetRaftEtagCommand : CommandBase
    {
        // This command is only to get a unique etag from the cluster (The RaftCommandIndex)
        // We using it for recreating indexes on a recreated database
        public GetRaftEtagCommand()
        {
            // for deserialization
        }
    }
}
