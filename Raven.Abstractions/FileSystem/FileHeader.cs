using Raven.Abstractions.Data;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.FileSystem
{
    public class FileHeader
    {
        public RavenJObject Metadata { get; private set; }

        public string Name { get; set; }
        public long? TotalSize { get; set; }
        public string HumaneTotalSize { get; set; }

        public string Path { get; private set; }
        public string Extension { get; private set; }

        public DateTimeOffset CreationDate { get; private set; }

        public DateTimeOffset LastModified { get; private set; }

        public Etag Etag { get; private set; }
    }

}
