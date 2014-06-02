using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.FileSystem
{
    public class SourceSynchronizationInformation
    {
        public Guid LastSourceFileEtag { get; set; }
        public string SourceServerUrl { get; set; }
        public Guid DestinationServerId { get; set; }

        public override string ToString()
        {
            return string.Format("LastSourceFileEtag: {0}", LastSourceFileEtag);
        }
    }
}
