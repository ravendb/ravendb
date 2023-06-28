using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Client;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Comparers;

public sealed class DocumentsComparer : IComparer<BlittableJsonReaderObject>
{
    private readonly OrderByField[] _orderByFields;
    private readonly bool _extractFromData;
    private readonly Random[] _randoms;

    public DocumentsComparer(OrderByField[] orderByFields, bool extractFromData, bool hasOrderByRandom)
    {
        _orderByFields = orderByFields;
        _extractFromData = extractFromData;
        _randoms = hasOrderByRandom == false
            ? null
            : GetRandom(_orderByFields);
    }
    
    private Random[] GetRandom(OrderByField[] orderByFields)
    {
        var randoms = new Random[orderByFields.Length];
        
        for (int idX = 0, randomIdx = 0; idX < _orderByFields.Length; ++idX)
        {
            if (orderByFields[idX].OrderingType != OrderByFieldType.Random)
                continue;

            ref var randomField = ref orderByFields[idX];
            randoms[randomIdx++] = randomField.Arguments is {Length: > 0}
                ? new Random((int)Sparrow.Hashing.XXHash32.CalculateRaw(randomField.Arguments[0].NameOrValue)) 
                :  Random.Shared;
        }
        return randoms;
    }


    public int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
    {
        for (var i = 0; i < _orderByFields.Length; i++)
        {
            ref var orderByField = ref _orderByFields[i];
            var cmp = CompareField(in orderByField, i, x, y);
            if (cmp != 0)
                return orderByField.Ascending ? cmp : -cmp;
        }

        return 0;
    }

    private int CompareField(in OrderByField order, int index, BlittableJsonReaderObject x, BlittableJsonReaderObject y)
    {
        switch (order.OrderingType)
        {
            case OrderByFieldType.Implicit:
            case OrderByFieldType.String:
                {
                    var xVal = GetString(x, order.Name, index);
                    var yVal = GetString(y, order.Name, index);
                    return string.Compare(xVal, yVal, StringComparison.OrdinalIgnoreCase);
                }
            case OrderByFieldType.Long:
                {
                    var hasX = TryGetLongValue(x, order.Name, index, out long xLng);
                    var hasY = TryGetLongValue(y, order.Name, index, out long yLng);
                    if (hasX == false && hasY == false)
                        return 0;
                    if (hasX == false)
                        return 1;
                    if (hasY == false)
                        return -1;
                    return xLng.CompareTo(yLng);
                }
            case OrderByFieldType.Double:
            case OrderByFieldType.Distance:
                {
                    var hasX = TryGetDoubleValue(x, order.Name, index, out double xDbl);
                    var hasY = TryGetDoubleValue(y, order.Name, index, out double yDbl);
                    if (hasX == false && hasY == false)
                        return 0;
                    if (hasX == false)
                        return 1;
                    if (hasY == false)
                        return -1;
                    return xDbl.CompareTo(yDbl);
                }
            case OrderByFieldType.AlphaNumeric:
                {
                    var xVal = GetString(x, order.Name, index);
                    var yVal = GetString(y, order.Name, index);
                    if (xVal == null && yVal == null)
                        return 0;
                    if (xVal == null)
                        return -1;
                    if (yVal == null)
                        return 1;
                    return AlphaNumericFieldComparator.StringAlphanumComparer.Instance.Compare(xVal, yVal);
                }
            case OrderByFieldType.Random:
                return _randoms[index].Next(int.MinValue, int.MaxValue);

            case OrderByFieldType.Custom:
                throw new NotSupportedInShardingException("Custom sorting is not supported in sharding as of yet");
            case OrderByFieldType.Score:
            default:
                throw new ArgumentException("Unknown OrderingType: " + order.OrderingType);
        }
    }

    private string GetString(BlittableJsonReaderObject blittable, string fieldName, int index)
    {
        if (_extractFromData)
        {
            blittable.TryGet(fieldName, out string value);
            return value;
        }

        if (blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
            ThrowIfCannotFindMetadata(blittable);

        if (metadata.TryGet(Constants.Documents.Metadata.Sharding.Querying.OrderByFields, out BlittableJsonReaderArray orderByFields) == false)
            ThrowIfCannotFindOrderByFields(metadata);

        return orderByFields[index]?.ToString();
    }

    private bool TryGetLongValue(BlittableJsonReaderObject blittable, string fieldName, int index, out long value)
    {
        if (_extractFromData)
        {
            return blittable.TryGetWithoutThrowingOnError(fieldName, out value);
        }

        if (blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
            ThrowIfCannotFindMetadata(blittable);

        if (metadata.TryGet(Constants.Documents.Metadata.Sharding.Querying.OrderByFields, out BlittableJsonReaderArray orderByFields) == false)
            ThrowIfCannotFindOrderByFields(metadata);

        var arrayValue = orderByFields[index];
        if (arrayValue is long v)
        {
            value = v;
            return true;
        }

        ThrowIfNotExpectedType("long", arrayValue);
        value = 0;
        return false;
    }

    private bool TryGetDoubleValue(BlittableJsonReaderObject blittable, string fieldName, int index, out double value)
    {
        if (_extractFromData)
        {
            return blittable.TryGetWithoutThrowingOnError(fieldName, out value);
        }

        if (blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
            ThrowIfCannotFindMetadata(blittable);

        if (metadata.TryGet(Constants.Documents.Metadata.Sharding.Querying.OrderByFields, out BlittableJsonReaderArray orderByFields) == false)
            ThrowIfCannotFindOrderByFields(metadata);

        var arrayValue = orderByFields[index];
        if (arrayValue is LazyNumberValue lnv)
        {
            value = lnv.ToDouble(CultureInfo.InvariantCulture);
            return true;
        }

        if (arrayValue is long)
        {
            value = Convert.ToDouble(arrayValue);
            return true;
        }

        ThrowIfNotExpectedType(nameof(LazyNumberValue), arrayValue);
        value = 0;
        return false;
    }

    private static void ThrowIfCannotFindMetadata(BlittableJsonReaderObject blittable)
    {
        throw new InvalidOperationException($"Missing metadata in document. Unable to find {Constants.Documents.Metadata.Key} in {blittable}");
    }

    private static void ThrowIfCannotFindOrderByFields(BlittableJsonReaderObject metadata)
    {
        throw new InvalidOperationException($"Unable to find {Constants.Documents.Metadata.Sharding.Querying.OrderByFields} in metadata: {metadata}");
    }

    private static void ThrowIfNotExpectedType(string expectedType, object actualValue)
    {
        throw new InvalidOperationException($"Expected to get type: {expectedType} but got: {actualValue} of type: {actualValue.GetType()}");
    }
}
