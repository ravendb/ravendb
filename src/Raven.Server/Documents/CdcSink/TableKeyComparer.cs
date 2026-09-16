using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.CdcSink;

internal sealed class TableKeyComparer : IEqualityComparer<(string Schema, string Table)>
{
    public static readonly TableKeyComparer Instance = new();

    public bool Equals((string Schema, string Table) x, (string Schema, string Table) y) =>
        string.Equals(x.Schema, y.Schema, StringComparison.OrdinalIgnoreCase) &&
        string.Equals(x.Table, y.Table, StringComparison.OrdinalIgnoreCase);

    public int GetHashCode((string Schema, string Table) obj)
    {
        var h1 = StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Schema ?? "");
        var h2 = StringComparer.OrdinalIgnoreCase.GetHashCode(obj.Table ?? "");
        return HashCode.Combine(h1, h2);
    }
}
