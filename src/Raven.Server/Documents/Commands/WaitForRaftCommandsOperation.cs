using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Commands
{
    public class WaitForRaftCommandsOperation : IMaintenanceOperation
    {
        private readonly List<long> _raftIndexes;

        public WaitForRaftCommandsOperation(long index) : this(new List<long>(1) {index})
        {

        }

        public WaitForRaftCommandsOperation(List<long> indexes)
        {
            _raftIndexes = indexes;
        }

        public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
        {
            return new WaitForIndexNotificationCommand(_raftIndexes);
        }
    }

    public class WaitForCommandsRequest
    {
        public List<long> RaftCommandIndexes { get; set; }
    }
}
