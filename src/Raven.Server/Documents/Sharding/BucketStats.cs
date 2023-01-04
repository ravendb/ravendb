using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding;

public class BucketStats : IDynamicJson
{
    public int Bucket;

    public long Size;

    public long NumberOfDocuments;

    public DateTime LastModified;

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue()
        {
            [nameof(Bucket)] = Bucket,
            [nameof(Size)] = Size,
            [nameof(NumberOfDocuments)] = NumberOfDocuments,
            [nameof(LastModified)] = LastModified,
        };
    }
}
