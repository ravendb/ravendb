using System;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.TimeSeries
{
    public class TimeseriesIncludesHelper
    {
        public static (TimeSeriesRangeType Type, TimeValue Time) ParseTime(MethodExpression expression, string queryText, BlittableJsonReaderObject parameters = null)
        {
            TimeSeriesRangeType type;
            TimeValue time;
            switch (QueryMethod.GetMethodType(expression.Name.ToString()))
            {
                case MethodType.Last:
                    type = TimeSeriesRangeType.Last;
                    time = QueryMethod.TimeSeries.LastWithTime(expression, queryText, parameters);
                    break;
                default:
                    throw new InvalidQueryException($"Got invalid type {expression.Name}.", queryText, parameters);
            }

            return (type, time);
        }

        public static (TimeSeriesRangeType Type, int Count) ParseCount(MethodExpression expression, string queryText, BlittableJsonReaderObject parameters = null)
        {
            TimeSeriesRangeType type;
            int count;
            switch (QueryMethod.GetMethodType(expression.Name.ToString()))
            {
                case MethodType.Last:
                    type = TimeSeriesRangeType.Last;
                    count = QueryMethod.TimeSeries.LastWithCount(expression, queryText, parameters);
                    break;
                default:
                    throw new InvalidQueryException($"Got invalid type {expression.Name}.", queryText, parameters);
            }

            return (type, count);
        }

        internal static string ExtractValueFromExpression(QueryExpression expression)
        {
            switch (expression)
            {
                case FieldExpression fe:
                    return fe.FieldValue;
                case ValueExpression ve:
                    return ve.GetValue(null)?.ToString();
                default:
                    throw new InvalidOperationException("Query only support include of fields, but got: " + expression);
            }
        }
    }
}
