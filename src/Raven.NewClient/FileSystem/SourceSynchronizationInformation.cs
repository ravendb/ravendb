using System;
using Raven.NewClient.Abstractions.Data;

namespace Raven.NewClient.Abstractions.FileSystem
{
    public class SourceSynchronizationInformation
    {
        public long? LastSourceFileEtag { get; set; }
        public string SourceServerUrl { get; set; }
        public Guid DestinationServerId { get; set; }

        public override string ToString()
        {
            return string.Format("LastSourceFileEtag: {0}", LastSourceFileEtag);
        }
    }
}
