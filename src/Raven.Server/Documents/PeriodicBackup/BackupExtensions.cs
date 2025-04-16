using System;
using Raven.Client.Documents.Operations.Backups;

public static class BackupExtensions
{
    public static Amazon.S3.S3StorageClass ToAmazonS3StorageClass(this S3StorageClass storageClass)
    {
        switch (storageClass)
        {
            case S3StorageClass.DeepArchive:
                return Amazon.S3.S3StorageClass.DeepArchive;
            case S3StorageClass.Glacier:
                return Amazon.S3.S3StorageClass.Glacier;
            case S3StorageClass.GlacierInstantRetrieval:
                return Amazon.S3.S3StorageClass.GlacierInstantRetrieval;
            case S3StorageClass.IntelligentTiering:
                return Amazon.S3.S3StorageClass.IntelligentTiering;
            case S3StorageClass.OneZoneInfrequentAccess:
                return Amazon.S3.S3StorageClass.OneZoneInfrequentAccess;
            case S3StorageClass.Outposts:
                return Amazon.S3.S3StorageClass.Outposts;
            case S3StorageClass.ReducedRedundancy:
                return Amazon.S3.S3StorageClass.ReducedRedundancy;
            case S3StorageClass.Standard:
                return Amazon.S3.S3StorageClass.Standard;
            case S3StorageClass.StandardInfrequentAccess:
                return Amazon.S3.S3StorageClass.StandardInfrequentAccess;
            case S3StorageClass.Snow:
                return Amazon.S3.S3StorageClass.Snow;
            case S3StorageClass.ExpressOneZone:
                return Amazon.S3.S3StorageClass.ExpressOnezone;
            default:
                throw new NotSupportedException($"Could not convert '{storageClass}' to Amazon S3 Storage Class.");
        }
    }
}
