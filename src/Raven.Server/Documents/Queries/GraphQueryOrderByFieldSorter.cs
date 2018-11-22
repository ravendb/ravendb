using System;
using System.Collections.Generic;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    internal class GraphQueryOrderByFieldSorter: IComparer<GraphQueryRunner.Match>
    {
        private OrderByField _field;
        private string _alias;
        private BlittablePath _path;

        public GraphQueryOrderByFieldSorter(OrderByField field)
        {
            var fieldName = field.Name.Value;
            var indexOfDot = fieldName.IndexOf('.');
            if (indexOfDot < 0)
                throw new NotSupportedException($"{GetType().Name} got an order by field: {fieldName} that isn't in the expected format of alias.fieldName");
            _alias = fieldName.Substring(0, indexOfDot);
            _path = new BlittablePath(fieldName.Substring(indexOfDot + 1, fieldName.Length - indexOfDot - 1));
            _field = field;
        }



        public int Compare(GraphQueryRunner.Match x, GraphQueryRunner.Match y)
        {
            int order = _field.Ascending ? 1 : -1;
            object xObject = null;
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
                        break;
                    }
                    ThrowMissmatchTypes(xResult, yResult);                   
                    break;
                case BlittableJsonReaderObject bjroX:
                    if (yResult is BlittableJsonReaderObject bjroY)
                    {
                        xObject = _path.Evaluate(bjroX, true);
                        yObject = _path.Evaluate(bjroY, true);
                        break;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                case LazyStringValue xLazyStringValue:
                    return xLazyStringValue.CompareTo(yResult) * order;
                case string xString:
                    if (yResult is string yString)
                    {
                        xObject = xString;
                        yObject = yString;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                case LazyNumberValue xLazyNumberValue:
                    return xLazyNumberValue.CompareTo(yResult) * order;                
                case int xInt:
                    if (yResult is int yInt)
                    {
                        xObject = xInt;
                        yObject = yInt;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                case long xLong:
                    if (yResult is long yLong)
                    {
                        xObject = xLong;
                        yObject = yLong;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                case double xDouble:
                    if (yResult is double yDouble)
                    {
                        xObject = xDouble;
                        yObject = yDouble;
                    }
                    ThrowMissmatchTypes(xResult, yResult);
                    break;
                default:
                    throw new NotSupportedException($"Got unexpected types for compare ${xResult} and ${yResult}");
            }

            switch (xObject)
            {
                case LazyStringValue xLazyStringValue:
                    return xLazyStringValue.CompareTo(yObject) * order;
                case string xString:
                    if (yObject is string yString)
                    {
                        return string.CompareOrdinal(xString, yString) * order;
                    }
                    ThrowMissmatchFieldsTypes(xObject, yObject);
                    break;
                case LazyNumberValue xLazyNumberValue:
                    return xLazyNumberValue.CompareTo(yObject) * order;
                case int xInt:
                    if (yObject is int yInt)
                    {
                        return xInt.CompareTo(yInt) * order;
                    }
                    ThrowMissmatchFieldsTypes(xObject, yObject);
                    break;
                case long xLong:
                    if (yObject is long yLong)
                    {
                        return xLong.CompareTo(yLong) * order;
                    }
                    ThrowMissmatchFieldsTypes(xObject, yObject);
                    break;
                case IComparable xComparable:
                    return xComparable.CompareTo(yObject) * order;
                default:
                    throw new NotSupportedException($"Can't compare two object ${xObject} and ${yObject}");
                    break;

            }
            return 1; //TODO: do actual compare here...
        }

        private void ThrowMissmatchFieldsTypes(object xObject, object yObject)
        {
            throw new NotSupportedException($"Can't compare two fields ${xObject} and ${yObject} of diffrent types");
        }

        private void ThrowMissmatchTypes(object xResult, object yResult)
        {
            throw new NotSupportedException($"Can't compare two object ${xResult} and ${yResult} of diffrent types");
        }
    }
}
