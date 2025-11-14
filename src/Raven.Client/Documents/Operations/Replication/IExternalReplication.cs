using System;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Replication
{
    internal interface IExternalReplication : IDynamicJson
    {
        bool Disabled { get; set; }

        long TaskId { get; set; }

        string Name { get; set; }

        string MentorNode { get; set; }
        
        bool PinToMentorNode { get; set; }

        TimeSpan DelayReplicationFor { get; set; }

        string GetDefaultTaskName();
    }
}
