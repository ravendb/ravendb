using System;

namespace Raven.Server.Documents.Sharding;

public class BucketStats
{
    public int Bucket;

    public long Size;

    public long NumberOfDocuments;

    public DateTime LastModified;
}
