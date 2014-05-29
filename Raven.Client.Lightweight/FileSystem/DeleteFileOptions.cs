using Raven.Abstractions.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem
{
    public class DeleteFileOptions
    {
        public bool FailIfNotExists = false;
        public Etag WithEtag = null;
    }
}
