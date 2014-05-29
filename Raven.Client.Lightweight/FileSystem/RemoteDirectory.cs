using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem
{
    public class RemoteDirectory : IRemoteObject
    {
        public string FullName { get; private set; }
    }
}
