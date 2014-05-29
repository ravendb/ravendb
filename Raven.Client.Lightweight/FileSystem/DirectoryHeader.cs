using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem
{
    public class DirectoryHeader 
    {
        public RavenJObject Metadata { get; private set; }

        public string FullName { get; private set; }
    }
}
