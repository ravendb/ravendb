using System;
using System.Collections.Generic;
using System.Xml.Linq;
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
        private readonly string _databaseName;
        private readonly string _query;
        private string _xId = Unknown;
        private string _yId = Unknown;
        private const string Unknown = "Unknown";

        public GraphQueryOrderByFieldComparer(OrderByField field, string databaseName, string query)
        {
            _databaseName = databaseName;
            _query = query;
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
            object yObject = null;
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
                        _xId = xDocument.Id;
                        _yId = yDocument.Id;
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
                        break;
                    }
                    else if(yResult is string yStringValue)
                    {
                        xObject = xLazyStringValue;
                        yObject = yStringValue;
                        break;
                    }
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
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
                case LazyNumberValue _:
                case int _:
                case long _:
                case double _:
                    xObject = xResult;
                    var (isNumber, compare) = SetObjectIfNumericType(xResult, yResult, ref yObject);
                    if (isNumber == false)
                        return compare;
                    break;
                default:
                    return LogMissmatchTypesReturnOrder(xResult, yResult);
            }

            return Compare(xObject, yObject);
        }

        private (bool IsNumber,int Compare) SetObjectIfNumericType(object xResult, object yResult, ref object yObject)
        {
            switch (yResult)
            {
                case int _:
                case long _:
                case double _:
                case float _:
                case LazyNumberValue _:
                    yObject = yResult;
                    break;
                default:
                    return (false, LogMissmatchTypesReturnOrder(xResult, yResult));
            }

            return (true, 0);
        }

        private int Compare(object xObject, object yObject )
        {
            //Null values will appear last
            if (xObject == null)
            {
                if (yObject == null)
                    return 0;

                return _order;
            }

            if (yObject == null)
            {
                return _order * -1;
            }

            switch (xObject)
            {
                case LazyStringValue xLazyStringValue:
                    if (yObject is LazyStringValue yLazyStringValue)
                    {
                        return xLazyStringValue.CompareTo(yLazyStringValue) * _order;
                    }
                    else if (yObject is string yStringValue)
                    {
                        return xLazyStringValue.CompareTo(yStringValue) * _order;
                    }

                    return LogMissmatchTypesReturnOrder(xObject, yObject);
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
                    switch (yObject)
                    {
                        case int _:
                        case long _:
                        case double _:
                        case float _:
                        case LazyNumberValue _:
                            return xLong.CompareTo(yObject) * _order;
                        default:
                            return LogMissmatchTypesReturnOrder(xObject, yObject);
                    }
                case int xInt:
                    switch (yObject)
                    {
                        case int _:
                        case long _:
                        case double _:
                        case float _:
                        case LazyNumberValue _:
                            return xInt.CompareTo(yObject) * _order;
                        default:
                            return LogMissmatchTypesReturnOrder(xObject, yObject);
                    }
                case double xDouble:
                    switch (yObject)
                    {
                        case int _:
                        case long _:
                        case double _:
                        case float _:
                        case LazyNumberValue _:
                            return xDouble.CompareTo(yObject) * _order;
                        default:
                            return LogMissmatchTypesReturnOrder(xObject, yObject);
                    }
                case float xFloat:
                    switch (yObject)
                    {
                        case int _:
                        case long _:
                        case double _:
                        case float _:
                        case LazyNumberValue _:
                            return xFloat.CompareTo(yObject) * _order;
                        default:
                            return LogMissmatchTypesReturnOrder(xObject, yObject);
                    }
                case BlittableJsonReaderArray xBlittableJsonReaderArray:
                    if (yObject is BlittableJsonReaderArray yBlittableJsonReaderArray)
                    {
                        return CompareBlittableArray(xBlittableJsonReaderArray, yBlittableJsonReaderArray);
                    }
                    return LogMissmatchTypesReturnOrder(xObject, yObject);
                case BlittableJsonReaderObject xBlittableJsonReaderObject:
                    if (yObject is BlittableJsonReaderObject yBlittableJsonReaderObject)
                    {
                        //We don't really support meaningful sort by blittable and we just need to provide a consistent result.
                        return xBlittableJsonReaderObject.Location.CompareTo(yBlittableJsonReaderObject.Location);                  
                    }

                    return LogMissmatchTypesReturnOrder(xObject, yObject);
                default:
                    return LogMissmatchTypesReturnOrder(xObject, yObject);
            }
        }

        private int CompareBlittableArray(BlittableJsonReaderArray xBlittableJsonReaderArray, BlittableJsonReaderArray yBlittableJsonReaderArray)
        {
            var xCurr = 0;
            var yCurr = 0;
            while (xCurr < xBlittableJsonReaderArray.Length && yCurr< yBlittableJsonReaderArray.Length)
            {
                var res = Compare(xBlittableJsonReaderArray[xCurr++], yBlittableJsonReaderArray[yCurr++]);
                //Here we don't multiply by order since the recursive call to Compare already applied order.
                if (res != 0)
                    return res;
            }

            if (xCurr == xBlittableJsonReaderArray.Length && yCurr == yBlittableJsonReaderArray.Length)
            {
                return 0;
            }

            return (xCurr - yCurr) * _order; //longer with equal prefix is bigger
        }

        private int LogMissmatchTypesReturnOrder(object xResult, object yResult)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info($"Database: {_databaseName} Graph Query: {_query} Document Ids:({_xId},{_yId}), Got unexpected types to compare: {xResult} of type {xResult.GetType().Name} and {yResult} of type {yResult.GetType().Name}, this may yield unexpected query ordering.");
            }

            return _order;
        }
    }

    internal class GraphQueryMultipleFieldsComparer : IComparer<GraphQueryRunner.Match>
    {
        private List<GraphQueryOrderByFieldComparer> _comparers;

        public GraphQueryMultipleFieldsComparer(IEnumerable<OrderByField> fields, string databaseName, string query)
        {
            _comparers = new List<GraphQueryOrderByFieldComparer>();
            foreach (var field in fields)
            {
                _comparers.Add(new GraphQueryOrderByFieldComparer(field, databaseName, query));
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
