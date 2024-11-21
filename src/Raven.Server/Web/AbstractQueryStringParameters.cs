using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Raven.Client;

namespace Raven.Server.Web;

internal abstract class AbstractQueryStringParameters(HttpRequest httpRequest)
{
    private readonly HttpRequest _httpRequest = httpRequest ?? throw new ArgumentNullException(nameof(httpRequest));

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

    protected static readonly ReadOnlyMemory<char> AddSpatialPropertiesQueryStringName = "addSpatialProperties".AsMemory();

    protected static readonly ReadOnlyMemory<char> IncludeServerSideQueryQueryStringName = "includeServerSideQuery".AsMemory();

    protected static readonly ReadOnlyMemory<char> DiagnosticsQueryStringName = "diagnostics".AsMemory();

    protected static readonly ReadOnlyMemory<char> AddTimeSeriesNamesQueryStringName = "addTimeSeriesNames".AsMemory();

    protected static readonly ReadOnlyMemory<char> DisableAutoIndexCreationQueryStringName = "disableAutoIndexCreation".AsMemory();

    protected static readonly ReadOnlyMemory<char> DebugQueryStringName = "debug".AsMemory();

    protected static readonly ReadOnlyMemory<char> IgnoreLimitQueryStringName = "ignoreLimit".AsMemory();

    protected static readonly ReadOnlyMemory<char> NoReplyQueryStringName = "noreply".AsMemory();

    protected static readonly ReadOnlyMemory<char> WaitForIndexesTimeoutQueryStringName = "waitForIndexesTimeout".AsMemory();

    protected static readonly ReadOnlyMemory<char> WaitForIndexThrowQueryStringName = "waitForIndexThrow".AsMemory();

    protected static readonly ReadOnlyMemory<char> WaitForSpecificIndexQueryStringName = "waitForSpecificIndex".AsMemory();

    protected static readonly ReadOnlyMemory<char> WaitForReplicasTimeoutQueryStringName = "waitForReplicasTimeout".AsMemory();

    protected static readonly ReadOnlyMemory<char> NumberOfReplicasToWaitForQueryStringName = "numberOfReplicasToWaitFor".AsMemory();

    protected static readonly ReadOnlyMemory<char> ThrowOnTimeoutInWaitForReplicasQueryStringName = "throwOnTimeoutInWaitForReplicas".AsMemory();

    private Dictionary<string, List<string>> _tempStringValues;

    protected void Parse()
    {
        foreach (var pair in new QueryStringEnumerable(_httpRequest.QueryString.Value))
            OnValue(pair);

        OnFinalize();

        _tempStringValues = null;
    }

    protected void AddForStringValues(string name, ReadOnlyMemory<char> value)
    {
        _tempStringValues ??= new Dictionary<string, List<string>>();

        ref var item = ref CollectionsMarshal.GetValueRefOrAddDefault(_tempStringValues, name, out bool exists);
        if (exists == false)
            item = new List<string>(1);

        item.Add(value.ToString());
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

    protected static bool GetBoolValue(ReadOnlyMemory<char> name, ReadOnlyMemory<char> value)
    {
        if (bool.TryParse(value.Span, out var result))
            return result;

        ThrowInvalidBool(name, value);
        return default;
    }

    protected static int GetIntValue(ReadOnlyMemory<char> name, ReadOnlyMemory<char> value)
    {
        if (int.TryParse(value.Span, out var result))
            return result;

        ThrowInvalidInt(name, value);
        return default;
    }

    protected static bool TryGetEnumValue<TEnum>(ReadOnlyMemory<char> value, out TEnum outValue)
        where TEnum : struct
    {
        return Enum.TryParse(value.Span, ignoreCase: true, out outValue);
    }

    protected static TimeSpan GetTimeSpan(ReadOnlyMemory<char> name, ReadOnlyMemory<char> value)
    {
        if (TimeSpan.TryParse(value.Span, out TimeSpan result))
            return result;

        ThrowInvalidTimeSpan(name, value);
        return default;
    }

    [DoesNotReturn]
    private static void ThrowInvalidTimeSpan(ReadOnlyMemory<char> name, ReadOnlyMemory<char> value)
    {
        throw new ArgumentException($"Could not parse query string '{name}' as timespan val {value}");
    }

    [DoesNotReturn]
    private static void ThrowInvalidBool(ReadOnlyMemory<char> name, ReadOnlyMemory<char> value)
    {
        throw new ArgumentException($"Could not parse query string '{name}' as bool val {value}");
    }

    [DoesNotReturn]
    private static void ThrowInvalidInt(ReadOnlyMemory<char> name, ReadOnlyMemory<char> value)
    {
        throw new ArgumentException($"Could not parse query string '{name}' as int val {value}");
    }
}
