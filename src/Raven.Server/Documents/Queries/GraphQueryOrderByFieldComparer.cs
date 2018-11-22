using System;
using System.Collections.Generic;
using Raven.Client.Json;
using Raven.Server.Documents.Patch;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents.Queries
{
    internal class GraphQueryOrderByFieldComparer: IComparer<GraphQueryRunner.Match>
    {
        private OrderByField _field;
        private string _alias;
        private BlittablePath _path;
        private int _order;
        public readonly Logger Log = LoggingSource.Instance.GetLogger<GraphQueryOrderByFieldComparer>("GraphQueryOrderByFieldComparer");

        public GraphQueryOrderByFieldComparer(OrderByField field)
        {
            var fieldName = field.Name.Value;
            var indexOfDot = fieldName.IndexOf('.');
            if (indexOfDot < 0)
                throw new NotSupportedException($"{GetType().Name} got an _order by field: {fieldName} that isn't in the expected format of alias.fieldName");
            _alias = fieldName.Substring(0, indexOfDot);
            _path = new BlittablePath(fieldName.Substring(indexOfDot + 1, fieldName.Length - indexOfDot - 1));
            _field = field;
        }



        public int Compare(GraphQueryRunner.Match x, GraphQueryRunner.Match y)
        {
            _order = _field.Ascending ? 1 : -1;
            object xObject;
            object yObject;
            var xResult = x.GetResult(_alias);
            var yResult = y.GetResult(_alias);
            switch (xResult)
            {
                case Document xDocument:
                    if (yResult is Document yDocument)
                    {
                        xDocument.EnsureMetadata();
                        yDocument.EnsureMetadata();
                        xObject = _path.Evaluate(xDocument.Data, true);
                        yObject = _path.Evaluate(yDocument.Data, true);
                        break;
                    }
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
                case BlittableJsonReaderObject bjroX:
                    if (yResult is BlittableJsonReaderObject bjroY)
                    {
                        xObject = _path.Evaluate(bjroX, true);
                        yObject = _path.Evaluate(bjroY, true);
                        break;
                    }
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
                case LazyStringValue xLazyStringValue:
                    if (yResult is LazyStringValue yLazyStringValue)
                    {
                        xObject = xLazyStringValue;
                        yObject = yLazyStringValue;
                    }
                    else if(yResult is string yStringValue)
                    {
                        xObject = xLazyStringValue;
                        yObject = yStringValue;
                        break;
                    }
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
                    break;
                case string xString:
                    if (yResult is string yString)
                    {
                        xObject = xString;
                        yObject = yString;
                        break;
                    } else if (yResult is LazyStringValue yLazyStringValue2)
                    {
                        xObject = xString;
                        yObject = yLazyStringValue2;
                        break;
                    }
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
                    break;
                case LazyNumberValue xLazyNumberValue:
                    xObject = xLazyNumberValue;
                    switch (yResult)
                    {
                        case int i:
                            yObject = i;
                            break;
                        case long l:
                            yObject = l;
                            break;
                        case double d:
                            yObject = d;
                            break;
                        case float f:
                            yObject = f;
                            break;
                        case LazyNumberValue lnv:
                            yObject = lnv;
                            break;
                        default:
                            return LogMissmatchTypesReturnOrder(xResult, yResult);
                    }                                        
                    break;
                case int xInt:
                    if (yResult is int yInt)
                    {
                        xObject = xInt;
                        yObject = yInt;
                        break;
                    }
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
                case long xLong:
                    if (yResult is long yLong)
                    {
                        xObject = xLong;
                        yObject = yLong;
                        break;
                    }
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
                case double xDouble:
                    if (yResult is double yDouble)
                    {
                        xObject = xDouble;
                        yObject = yDouble;
                        break;
                    }
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
                default:
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
            }

            switch (xObject)
            {
                case LazyStringValue xLazyStringValue:
                    if (yObject is LazyStringValue yLazyStringValue)
                    {
                        return xLazyStringValue.CompareTo(yLazyStringValue) * _order;
                    }
                    else if (yResult is string yStringValue)
                    {
                        return xLazyStringValue.CompareTo(yStringValue) * _order;
                    }
                    return LogMissmatchTypesReturnOrder(xObject, yObject);
                    break;
                case string xString:
                    if (yObject is string yString)
                    {
                        return string.CompareOrdinal(xString, yString) * _order;
                    }
                    return LogMissmatchTypesReturnOrder(xObject, yObject);
                case LazyNumberValue xLazyNumberValue:
                    switch (yObject)
                    {
                        case int i:
                            return xLazyNumberValue.CompareTo(i) * _order;
                        case long l:
                            return xLazyNumberValue.CompareTo(l) * _order;
                        case double d:
                            return xLazyNumberValue.CompareTo(d) * _order;
                        case float f:
                            return xLazyNumberValue.CompareTo(f) * _order;
                        case LazyNumberValue lnv:
                            return xLazyNumberValue.CompareTo(lnv) * _order;
                        default:
                            return LogMissmatchTypesReturnOrder(xObject, yObject);
                    }
                case long xLong:
                    if (yObject is long yLong)
                    {
                        return xLong.CompareTo(yLong) * _order;
                    }
                    return LogMissmatchTypesReturnOrder(xObject, yObject);
                case IComparable xComparable:
                    if(yObject.GetType() == xObject.GetType())
                        return xComparable.CompareTo(yObject) * _order;
                    return LogMissmatchTypesReturnOrder(xObject, yObject);
                case BlittableJsonReaderArray xBlittableJsonReaderArray:
                    if (yObject is BlittableJsonReaderArray yBlittableJsonReaderArray)
                    {
                        return string.CompareOrdinal(xBlittableJsonReaderArray.ToString(), yBlittableJsonReaderArray.ToString());
                    }
                    return LogMissmatchTypesReturnOrder(xObject, yObject);
                case BlittableJsonReaderObject xBlittableJsonReaderObject:
                    if (yObject is BlittableJsonReaderObject yBlittableJsonReaderObject)
                    {
                        return string.CompareOrdinal(xBlittableJsonReaderObject.ToString(), yBlittableJsonReaderObject.ToString());
                    }
                    return LogMissmatchTypesReturnOrder(xObject, yObject);
                default:
                    return LogMissmatchTypesReturnOrder(xObject, yObject);
            }
        }

        private int LogMissmatchTypesReturnOrder(object xResult, object yResult)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info($"Got unexpected types for compare ${xResult} of type {xResult.GetType().Name} and ${yResult} of type {yResult.GetType().Name}.");
            }

            return _order;
        }
    }

    internal class GraphQueryMultipleFieldsComparer : IComparer<GraphQueryRunner.Match>
    {
        private List<GraphQueryOrderByFieldComparer> _comparers;

        public GraphQueryMultipleFieldsComparer(IEnumerable<OrderByField> fields)
        {
            _comparers = new List<GraphQueryOrderByFieldComparer>();
            foreach (var field in fields)
            {
                _comparers.Add(new GraphQueryOrderByFieldComparer(field));
            }
        }

        public int Compare(GraphQueryRunner.Match x, GraphQueryRunner.Match y)
        {
            foreach (var comparer in _comparers)
            {
                var res = comparer.Compare(x, y);
                if (res != 0)
                    return res;
            }

            return 0;
        }
    }
}
