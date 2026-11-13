using System.Runtime.CompilerServices;
using System.Text;
using Corax.Querying.Planning;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    internal static ScanValueType ClassifyParamType(BlittableJsonReaderObject queryParams, string name)
    {
        if (queryParams.TryGet(name, out object raw) == false || raw == null)
            return ScanValueType.Slice;
        return ClassifyValue(raw);

        static ScanValueType ClassifyValue(object raw)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            return raw switch
            {
                long => ScanValueType.Long,
                double => ScanValueType.Double,
                LazyNumberValue lnv => lnv.TryParseLong(out _) ? ScanValueType.Long : ScanValueType.Double,
                string { Length: < 83 } => ScanValueType.Slice, // statically skip Encoding.UTF8.GetByteCount() < 255 here, since we _know_ it's < 255 regardless
                // <= 255 bytes is compound-field-eligible (Slice); match the LazyStringValue branch's boundary exactly.
                string s when Encoding.UTF8.GetByteCount(s) <= byte.MaxValue => ScanValueType.Slice,
                // we distinguish strings > 255 bytes because they cannot use compound field optimizations, so this ensures that we have a separate plan for them
                string => ScanValueType.SliceLong,
                LazyStringValue lsv => lsv.Size > byte.MaxValue ? ScanValueType.SliceLong : ScanValueType.Slice,
                BlittableJsonReaderArray arr => arr.Length > 0 ? ClassifyValue(arr[0]) : ScanValueType.Slice,
                _ => ScanValueType.Slice
            };
        }
    }
}
