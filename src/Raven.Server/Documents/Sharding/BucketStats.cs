using System;

namespace Raven.Server.Documents.Sharding;

public class BucketStats
{
    public int Bucket;

    public long Size;

    public long NumberOfItems;

    public DateTime LastModified;
}
