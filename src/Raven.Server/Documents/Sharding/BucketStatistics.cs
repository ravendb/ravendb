using System;

namespace Raven.Server.Documents.Sharding;

public class BucketStatistics
{
    public int Bucket;

    public long Size;

    public long NumberOfItems;

    public DateTime LastModified;
}
