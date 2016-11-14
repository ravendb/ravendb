using Raven.NewClient.Abstractions.Data;

namespace Raven.NewClient.Abstractions.FileSystem
{
    public class SynchronizationDetails
    {
        public string FileName { get; set; }
        public long? FileETag { get; set; }
        public string DestinationUrl { get; set; }
        public SynchronizationType Type { get; set; }
    }
}
