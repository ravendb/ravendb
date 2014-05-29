using Raven.Abstractions.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem
{
    public class DeleteDirectoryOptions
    {
        public bool FailIfNotExists = false;
        public Etag WithEtag = null;

        public bool Recursive = false;
    }
}
