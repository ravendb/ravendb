using System;
using System.Collections.Generic;
using System.Globalization;
using Raven.Client;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Queries;

public class ShardedDocumentsComparer : IComparer<BlittableJsonReaderObject>
{
    private readonly QueryMetadata _metadata;
    private readonly bool _isMapReduce;

    public ShardedDocumentsComparer(QueryMetadata metadata, bool isMapReduce)
    {
        _metadata = metadata;
        _isMapReduce = isMapReduce;
    }

    public int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
    {
        for (var i = 0; i < _metadata.OrderBy.Length; i++)
        {
            var orderByField = _metadata.OrderBy[i];
            var cmp = CompareField(orderByField, i, x, y);
            if (cmp != 0)
                return orderByField.Ascending ? cmp : -cmp;
        }

        return 0;
    }

    private int CompareField(OrderByField order, int index, BlittableJsonReaderObject x, BlittableJsonReaderObject y)
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
                    return 1;
                if (yVal == null)
                    return -1;
                return AlphaNumericFieldComparator.StringAlphanumComparer.Instance.Compare(xVal, yVal);
            }
            case OrderByFieldType.Random:
                return Random.Shared.Next(0, int.MaxValue);

            case OrderByFieldType.Custom:
                throw new NotSupportedException("Custom sorting is not supported in sharding as of yet");
            case OrderByFieldType.Score:
            case OrderByFieldType.Distance:
            default:
                throw new ArgumentException("Unknown OrderingType: " + order.OrderingType);
        }

            
    }

    private string GetString(BlittableJsonReaderObject blittable, string fieldName, int index)
    {
        if (_isMapReduce)
        {
            blittable.TryGet(fieldName, out string value);
            return value;
        }

        if (blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
            ThrowIfCannotFindMetadata(blittable);

        if (metadata.TryGet(Constants.Documents.Metadata.OrderByFields, out BlittableJsonReaderArray orderByFields) == false)
            ThrowIfCannotFindOrderByFields(metadata);

        return orderByFields[index].ToString();
    }

    private bool TryGetLongValue(BlittableJsonReaderObject blittable, string fieldName, int index, out long value)
    {
        if (_isMapReduce)
        {
            return blittable.TryGetWithoutThrowingOnError(fieldName, out value);
        }

        if (blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
            ThrowIfCannotFindMetadata(blittable);

        if (metadata.TryGet(Constants.Documents.Metadata.OrderByFields, out BlittableJsonReaderArray orderByFields) == false)
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
        if (_isMapReduce)
        {
            return blittable.TryGetWithoutThrowingOnError(fieldName, out value);
        }

        if (blittable.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
            ThrowIfCannotFindMetadata(blittable);

        if (metadata.TryGet(Constants.Documents.Metadata.OrderByFields, out BlittableJsonReaderArray orderByFields) == false)
            ThrowIfCannotFindOrderByFields(metadata);

        var arrayValue = orderByFields[index];
        if (arrayValue is LazyNumberValue lnv)
        {
            value = lnv.ToDouble(CultureInfo.InvariantCulture);
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
        throw new InvalidOperationException($"Unable to find {Constants.Documents.Metadata.OrderByFields} in metadata: {metadata}");
    }

    private static void ThrowIfNotExpectedType(string expectedType, object actualValue)
    {
        throw new InvalidOperationException($"Expected to get type: {expectedType} but got: {actualValue} of type: {actualValue.GetType()}");
    }
}
