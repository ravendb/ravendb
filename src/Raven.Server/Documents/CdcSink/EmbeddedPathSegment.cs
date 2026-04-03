using System.Collections.Generic;
using Raven.Client.Documents.Operations.CdcSink;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// One step along the path from a root document to an embedded location.
/// </summary>
public class EmbeddedPathSegment
{
    /// <summary>
    /// The embedded table configuration at this level.
    /// </summary>
    public CdcSinkEmbeddedTableConfig Config { get; set; }

    /// <summary>
    /// Maps FK column in child → PK column in parent, used for matching.
    /// </summary>
    public Dictionary<string, string> JoinMapping { get; set; }

    /// <summary>
    /// Pre-computed mapping of SQL column name → mapped property Name for this segment's columns.
    /// Used for primary key lookups during embedded path navigation.
    /// </summary>
    public Dictionary<string, string> PropertyLookup { get; set; }
}
