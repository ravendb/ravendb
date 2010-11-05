using System;

namespace Raven.Munin
{
    [Serializable]
    public class RemoteManagedStorageState
    {
        public string Path { get; set; }
        public string Prefix { get; set; }

        public byte[] Log { get; set; }
    }
}