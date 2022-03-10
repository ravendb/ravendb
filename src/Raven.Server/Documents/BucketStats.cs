using System;

namespace Raven.Server.Documents
{
    public class BucketStats
    {
        public int BucketId;
        public int Size;
        public int Count;
        public DateTime LastAccessed;
    }
}
