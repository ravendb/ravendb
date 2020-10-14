using System;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Replication
{
    public interface IExternalReplicationBase : IDynamicJsonValueConvertible
    {
        bool Disabled { get; set; }

        long TaskId { get; set; }

        string Name { get; set; }

        string MentorNode { get; set; }

        TimeSpan DelayReplicationFor { get; set; }

        string GetDefaultTaskName();
    }
}
