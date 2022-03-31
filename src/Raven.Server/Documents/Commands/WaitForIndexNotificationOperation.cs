using System.Collections.Generic;

namespace Raven.Server.Documents.Commands
{
    public class WaitForIndexNotificationRequest
    {
        public List<long> RaftCommandIndexes { get; set; }
    }
}
