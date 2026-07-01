namespace Raven.Client.Documents.Operations.CdcSink;

public enum CdcSinkRelationType
{
    /// <summary>
    /// One-to-many: stored as a JSON array.
    /// </summary>
    Array,

    /// <summary>
    /// One-to-many: stored as a JSON object keyed by primary key value(s).
    /// For composite PKs, the key is "pk1,pk2".
    /// </summary>
    Map,

    /// <summary>
    /// Many-to-one: stored as a single value/object.
    /// </summary>
    Value
}
