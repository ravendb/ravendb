namespace Raven.Client.Documents.Operations.Backups;

public enum S3StorageClass
{
    /// <summary>
    /// S3 Glacier Deep Archive provides secure, durable object storage class for long term data archival.
    /// It is the ideal storage class to make an archival, durable copy of data that rarely, if ever, needs to be accessed.
    /// It can be used as an offline backup for their most important data assets and to meet long-term retention needs.
    /// </summary>
    DeepArchive,

    /// <summary>
    /// The GLACIER storage is for object that are stored in Amazon Glacier.
    /// This storage class is for objects that are for archival purpose and 
    /// get operations are rare.
    /// <para></para>
    /// Durability 99.999999999%
    /// </summary>
    Glacier,

    /// <summary>
    /// Constant GLACIER_IR for ObjectStorageClass
    /// </summary>
    GlacierInstantRetrieval,

    /// <summary>
    /// IntelligentTiering makes it easy to lower your overall cost of storage by automatically placing data in the storage
    /// class that best matches the access patterns for the storage. With IntelligentTiering, you don’t need to define
    /// and manage individual policies for lifecycle data management or write code to transition objects
    /// between storage classes. Instead, you can use IntelligentTiering to manage transitions between Standard and
    /// S-IA without writing any application code. IntelligentTiering also manages transitions automatically to
    /// Glacier for long term archive in addition to S3 storage classes.
    /// </summary>
    IntelligentTiering,

    /// <summary>
    /// The ONEZONE_IA storage is for infrequently accessed objects. It is similiar to STANDARD_IA, but
    /// only stores object data within one Availablity Zone in a given region.
    /// <para></para>
    /// Durability 99.999999999%; Availability 99% over a given year.
    /// </summary>
    OneZoneInfrequentAccess,

    /// <summary>
    /// The OUTPOSTS storage class for objects stored in a S3 Outpost
    /// </summary>
    Outposts,

    /// <summary>
    /// REDUCED_REDUNDANCY provides the same availability as standard, but at a lower durability.
    /// <para></para>
    /// Durability 99.99%; Availability 99.99% over a given year.
    /// </summary>
    ReducedRedundancy,

    /// <summary>
    /// The STANDARD storage class, which is the default
    /// storage class for S3.
    /// <para></para>
    /// Durability 99.999999999%; Availability 99.99% over a given year.
    /// </summary>
    Standard,

    /// <summary>
    /// The STANDARD_IA storage is for infrequently accessed objects.
    /// This storage class is for objects that are long-lived and less frequently accessed,
    /// like backups and older data.
    /// <para></para>
    /// Durability 99.999999999%; Availability 99.9% over a given year.
    /// </summary>
    StandardInfrequentAccess,

    /// <summary>
    /// The SNOW storage is for objects stored in Amazon S3 compatible object storage
    /// for Snow family devices.
    /// </summary>
    Snow,

    /// <summary>
    /// The EXPRESS_ONEZONE storage class for faster access to S3
    /// </summary>
    ExpressOneZone,
}
