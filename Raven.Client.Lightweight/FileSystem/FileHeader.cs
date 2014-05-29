using Raven.Abstractions.Data;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem
{
    public class FileHeader
    {
        public RavenJObject Metadata { get; private set; }

        public string Path { get; private set; }
        public string FullName { get; private set; }
        public string Extension { get; private set; }

        public DateTimeOffset CreationDate { get; private set; }

        public DateTimeOffset LastModified { get; private set; }

        public Etag Etag { get; private set; }       
    }

}
