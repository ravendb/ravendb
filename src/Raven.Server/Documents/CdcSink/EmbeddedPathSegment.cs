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
}
