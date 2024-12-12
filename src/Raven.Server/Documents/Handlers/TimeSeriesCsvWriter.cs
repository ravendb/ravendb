using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers;

public class TimeSeriesCsvWriter : IDisposable
{
    public const string TimeSeriesPathPrefix = "$TS";
    private readonly IEnumerator<DynamicJsonValue> _it;

    public TimeSeriesCsvWriter(TimeSeriesStream tStream)
    {
        if (tStream == null)
        {
            // no time-series here
            return;
        }

        _it = tStream.TimeSeries.GetEnumerator();
        _it.MoveNext();
    }

    public bool MoveNext()
    {
        if (_it == null)
            return false; // no time-series here

        return _it.MoveNext();
    }

    public IEnumerable<(string Property, string Path)> GetProperties()
    {
        if (_it == null)
            return Enumerable.Empty<(string Property, string Path)>();

        using (var ctx = JsonOperationContext.ShortTermSingleUse())
        {
            var blittable = ctx.ReadObject(_it.Current, "ts-headers");
            return StreamCsvDocumentQueryResultWriter.GetPropertiesRecursive((string.Empty, TimeSeriesPathPrefix), blittable, addId: false).ToArray();
        }
    }

    public string GetValue(string key)
    {
        var p = _it?.Current[key];

        switch (p)
        {
            case DynamicJsonArray dja:
                var sb = new StringBuilder();
                sb.Append("[");
                for (int i = 0; i < dja.Items.Count; i++)
                {
                    object item = dja.Items[i];
                    sb.Append(item);
                    if (i < dja.Items.Count - 1)
                    {
                        sb.Append(",");
                    }
                }
                sb.Append("]");
                return sb.ToString();
            case DateTime dt:
                return dt.ToString("O");
            default:
                return p?.ToString();
        }
    }
    public void Dispose()
    {
        using (_it)
        {
                    
        }
    }
}
