using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.CdcSink.Test;

/// <summary>
/// One row's worth of output from <c>POST /admin/cdc-sink/test</c>: the synthesized
/// document ID, the post-mapping (and post-patch, if any) document, the original source
/// row, and any per-row debug output or error.
/// </summary>
/// <remarks>
/// <see cref="Document"/> and <see cref="SourceRow"/> are returned as JSON text rather
/// than parsed objects. Callers parse them in their own JSON context — keeps the response
/// independent of the request-executor's transient context lifetime. Studio's <c>JSON.parse</c>
/// and .NET's preferred JSON library (System.Text.Json / Newtonsoft.Json) both work.
/// </remarks>
internal class TestCdcSinkRowResult : IDynamicJson
{
    public string DocumentId { get; set; }

    /// <summary>The mapped (and patched, if a <c>Patch</c> script ran) document, as JSON text.
    /// Stays the pre-patch mapping if the script called <c>del()</c> or <c>put()</c>.</summary>
    public string Document { get; set; }

    /// <summary>Raw source-row values keyed by column name, as JSON text — for the Studio "show me the source row" affordance.</summary>
    public string SourceRow { get; set; }

    /// <summary>True for <c>Operation = Delete</c> when <c>OnDelete.IgnoreDeletes</c> is false. Always false for <c>Operation = Upsert</c>.</summary>
    public bool WouldDelete { get; set; }

    /// <summary>Mirrors <c>OnDelete.IgnoreDeletes</c> on the target table for the Studio UI's convenience.</summary>
    public bool IgnoreDeletes { get; set; }

    /// <summary>Captured <c>output(...)</c> calls from the patch script for this row.</summary>
    public List<string> DebugOutput { get; set; }

    /// <summary>Per-row error (e.g. the patch script threw). Null on success.</summary>
    public string Error { get; set; }

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(DocumentId)] = DocumentId,
            [nameof(Document)] = Document,
            [nameof(SourceRow)] = SourceRow,
            [nameof(WouldDelete)] = WouldDelete,
            [nameof(IgnoreDeletes)] = IgnoreDeletes,
            [nameof(DebugOutput)] = DebugOutput == null ? null : new DynamicJsonArray(DebugOutput),
            [nameof(Error)] = Error,
        };
    }
}
