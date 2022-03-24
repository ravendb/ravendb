using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands
{
    public class WaitForRaftCommandOperation : IMaintenanceOperation
    {
        private readonly long _raftIndex;

        public WaitForRaftCommandOperation(long index)
        {
            _raftIndex = index;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new WaitForRaftCommands(new List<long> { _raftIndex });
        }
    }

    public class WaitForRaftCommandsOperation : IMaintenanceOperation
    {
        private readonly List<long> _raftIndexes;
        public WaitForRaftCommandsOperation(List<long> indexes)
        {
            _raftIndexes = indexes;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new WaitForRaftCommands(_raftIndexes);
        }
    }

    public class WaitForCommandsRequest
    {
        public List<long> RaftCommandIndexes { get; set; }
    }
}
