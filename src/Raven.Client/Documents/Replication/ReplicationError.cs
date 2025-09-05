using System;

namespace Raven.Client.Documents.Replication
{
    public sealed class ReplicationError
    {
        public string Error { get; set; }
        public DateTime Timestamp { get; set; }

        public override string ToString()
        {
            return $"Error: {Error}";
        }
    }
}