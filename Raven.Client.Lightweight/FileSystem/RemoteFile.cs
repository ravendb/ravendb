using Raven.Abstractions.Data;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem
{
    public class IRemoteObject
    {

    }

    public class RemoteFile : IRemoteObject
    {
        public RemoteDirectory Directory { get; private set; }

        public string FullName { get; private set; }
        public string Extension { get; private set; }

        public DateTimeOffset CreationDate { get; private set; }

        public DateTimeOffset LastModified { get; private set; }

        public Etag Etag { get; private set; }

    }
}
