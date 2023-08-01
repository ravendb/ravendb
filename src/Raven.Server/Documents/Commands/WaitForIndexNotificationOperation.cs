using System.Collections.Generic;

namespace Raven.Server.Documents.Commands
{
    public sealed class WaitForIndexNotificationRequest
    {
        public List<long> RaftCommandIndexes { get; set; }
    }
}
