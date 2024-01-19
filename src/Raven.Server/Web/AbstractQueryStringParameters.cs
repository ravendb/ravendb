using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Raven.Client;

namespace Raven.Server.Web;

internal abstract class AbstractQueryStringParameters
{
    private readonly HttpRequest _httpRequest;

    protected static readonly ReadOnlyMemory<char> MetadataOnlyQueryStringName = "metadataOnly".AsMemory();

    protected static readonly ReadOnlyMemory<char> IncludesQueryStringName = "include".AsMemory();

    protected static readonly ReadOnlyMemory<char> IdQueryStringName = "id".AsMemory();

    protected static readonly ReadOnlyMemory<char> TxModeQueryStringName = "txMode".AsMemory();

    protected static readonly ReadOnlyMemory<char> CounterQueryStringName = "counter".AsMemory();

    protected static readonly ReadOnlyMemory<char> RevisionsQueryStringName = "revisions".AsMemory();

    protected static readonly ReadOnlyMemory<char> RevisionsBeforeQueryStringName = "revisionsBefore".AsMemory();

    protected static readonly ReadOnlyMemory<char> TimeSeriesQueryStringName = "timeseries".AsMemory();

    protected static readonly ReadOnlyMemory<char> AllTimeSeries = Constants.TimeSeries.All.AsMemory();

    protected static readonly ReadOnlyMemory<char> TimeSeriesTimesQueryStringName = "timeseriestime".AsMemory();

    protected static readonly ReadOnlyMemory<char> TimeSeriesCountsQueryStringName = "timeseriescount".AsMemory();

    protected static readonly ReadOnlyMemory<char> CmpxchgQueryStringName = "cmpxchg".AsMemory();

    protected static readonly ReadOnlyMemory<char> FromQueryStringName = "from".AsMemory();

    protected static readonly ReadOnlyMemory<char> ToQueryStringName = "to".AsMemory();

    protected static readonly ReadOnlyMemory<char> TimeTypeQueryStringName = "timeType".AsMemory();

    protected static readonly ReadOnlyMemory<char> TimeValueQueryStringName = "timeValue".AsMemory();

    protected static readonly ReadOnlyMemory<char> TimeUnitQueryStringName = "timeUnit".AsMemory();

    protected static readonly ReadOnlyMemory<char> CountTypeQueryStringName = "countType".AsMemory();

    protected static readonly ReadOnlyMemory<char> CountValueQueryStringName = "countValue".AsMemory();

    private Dictionary<string, List<string>> _tempStringValues;

    protected AbstractQueryStringParameters([NotNull] HttpRequest httpRequest)
    {
        _httpRequest = httpRequest ?? throw new ArgumentNullException(nameof(httpRequest));
    }

    protected void Parse()
    {
        foreach (var pair in new QueryStringEnumerable(_httpRequest.QueryString.Value))
            OnValue(pair);

        OnFinalize();
    }

    protected void AddForStringValues(string name, ReadOnlyMemory<char> value)
    {
        _tempStringValues ??= new Dictionary<string, List<string>>();
        if (_tempStringValues.TryGetValue(name, out var list) == false)
            _tempStringValues[name] = list = new List<string>(1);

        list.Add(value.ToString());
    }

    protected bool AnyStringValues() => _tempStringValues is { Count: > 0 };

    protected StringValues ConvertToStringValues(string name)
    {
        if (_tempStringValues == null || _tempStringValues.TryGetValue(name, out var list) == false)
            return default;

        return new StringValues(list.ToArray());
    }

    protected abstract void OnFinalize();

    protected abstract void OnValue(QueryStringEnumerable.EncodedNameValuePair pair);

    protected static bool IsMatch(ReadOnlyMemory<char> name, ReadOnlyMemory<char> expectedName)
    {
        return name.Span.Equals(expectedName.Span, StringComparison.OrdinalIgnoreCase);
    }

    protected static bool GetBoolValue(ReadOnlyMemory<char> value)
    {
        return bool.Parse(value.Span);
    }

    protected static bool TryGetEnumValue<TEnum>(ReadOnlyMemory<char> value, out TEnum outValue)
        where TEnum : struct
    {
        return Enum.TryParse(value.Span, ignoreCase: true, out outValue);
    }
}
