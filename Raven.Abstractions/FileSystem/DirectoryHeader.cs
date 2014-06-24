using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Abstractions.FileSystem
{
    public class DirectoryHeader
    {
        public RavenJObject Metadata { get; private set; }

        public string Name { get; set; }
        public string Path { get; private set; }

        public int Files { get; private set; }
    }
}
