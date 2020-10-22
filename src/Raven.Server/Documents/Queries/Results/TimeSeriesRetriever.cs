using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results
{
    public class TimeSeriesRetriever
    {
        private readonly Dictionary<ValueExpression, object> _valuesDictionary;

        private readonly Dictionary<FieldExpression, object> _argumentValuesDictionary;

        private readonly BlittableJsonReaderObject _queryParameters;

        private readonly DocumentsOperationContext _context;

        private Dictionary<string, Document> _loadedDocuments;
        private readonly bool _isFromStudio;

        private string _source;
        private string _collection;

        private string[] _namedValues;
        private bool _configurationFetched;

        private (long Count, DateTime Start, DateTime End) _stats;

        private static TimeSeriesAggregation[] AllAggregationTypes() =>  new[]
        {
            new TimeSeriesAggregation(AggregationType.First),
            new TimeSeriesAggregation(AggregationType.Last),
            new TimeSeriesAggregation(AggregationType.Min),
            new TimeSeriesAggregation(AggregationType.Max),
            new TimeSeriesAggregation(AggregationType.Sum),
            new TimeSeriesAggregation(AggregationType.Count),
            new TimeSeriesAggregation(AggregationType.Average),
        };

        public TimeSeriesRetriever(DocumentsOperationContext context, BlittableJsonReaderObject queryParameters, Dictionary<string, Document> loadedDocuments,
            bool isFromStudio)
        {
            _context = context;
            _queryParameters = queryParameters;
            _loadedDocuments = loadedDocuments;
            _isFromStudio = isFromStudio;

            _valuesDictionary = new Dictionary<ValueExpression, object>();
            _argumentValuesDictionary = new Dictionary<FieldExpression, object>();
        }

        
        public BlittableJsonReaderObject InvokeTimeSeriesFunction(DeclaredFunction declaredFunction, string documentId, object[] args, bool addProjectionToResult = false)
        {
            var timeSeriesFunction = declaredFunction.TimeSeries;
            
            _source = GetSourceAndId();

            _stats = _context.DocumentDatabase.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_context, documentId, _source);
            if (_stats.Count == 0)
                return GetFinalResult(null, 0, addProjectionToResult);
            
            _collection = GetCollection(documentId);

            var offset = GetOffset(timeSeriesFunction.Offset, declaredFunction.Name);
            var (from, to) = GetFromAndTo(declaredFunction, documentId, args, timeSeriesFunction, offset);

            var groupBy = timeSeriesFunction.GroupBy?.GetValue(_queryParameters)?.ToString();
            RangeGroup rangeSpec;
            if (groupBy != null)
            {
                rangeSpec = RangeGroup.ParseRangeFromString(groupBy);
            }
            else
            {
                rangeSpec = new RangeGroup();
                rangeSpec.InitializeFullRange(from, to);
            }
            var reader = groupBy == null
                ? new TimeSeriesReader(_context, documentId, _source, from, to, offset)
                : new TimeSeriesMultiReader(_context, documentId, _source, _collection, from, to, offset, rangeSpec.ToTimeValue()) as ITimeSeriesReader;

            var array = new DynamicJsonArray();

            if (timeSeriesFunction.GroupBy == null && timeSeriesFunction.Select == null)
                return GetRawValues();

            TimeSeriesAggregation[] aggStates;
            if (timeSeriesFunction.Select != null)
            {
                aggStates = new TimeSeriesAggregation[timeSeriesFunction.Select.Count];
                InitializeAggregationStates(timeSeriesFunction, aggStates);
            }
            else
            {
                aggStates = AllAggregationTypes();
            }

            return GetAggregatedValues();

            void AggregateIndividualItems(IEnumerable<SingleResult> items)
            {
                foreach (var cur in items)
                {
                    MaybeMoveToNextRange(cur.Timestamp);

                    if (ShouldFilter(cur, timeSeriesFunction.Where))
                        continue;

                    for (int i = 0; i < aggStates.Length; i++)
                    {
                        aggStates[i].Step(cur.Values.Span, reader.IsRaw);
                    }
                }
            }

            void MaybeMoveToNextRange(DateTime ts)
            {
                if (rangeSpec.WithinRange(ts))
                    return;

                if (aggStates[0].Any)
                {
                    array.Add(AddTimeSeriesResult(timeSeriesFunction, aggStates, rangeSpec.Start, rangeSpec.End));
                }

                rangeSpec.MoveToNextRange(ts);

                for (int i = 0; i < aggStates.Length; i++)
                {
                    aggStates[i].Init();
                }
            }

            BlittableJsonReaderObject GetRawValues()
            {
                var count = 0L;
                foreach (var singleResult in reader.AllValues())
                {
                    if (ShouldFilter(singleResult, timeSeriesFunction.Where))
                        continue;

                    var vals = new DynamicJsonArray();
                    for (var index = 0; index < singleResult.Values.Span.Length; index++)
                    {
                        vals.Add(singleResult.Values.Span[index]);
                    }

                    array.Add(new DynamicJsonValue
                    {
                        [nameof(TimeSeriesEntry.Tag)] = singleResult.Tag?.ToString(),
                        [nameof(TimeSeriesEntry.Timestamp)] = singleResult.Timestamp,
                        [nameof(TimeSeriesEntry.Values)] = vals,
                        [nameof(TimeSeriesEntry.IsRollup)] = singleResult.Type == SingleResultType.RolledUp,
                    });
                    count += reader.IsRaw ? 1 : (long)singleResult.Values.Span[(int)AggregationType.Count];
                }

                _argumentValuesDictionary.Clear();
                return GetFinalResult(array, count, addProjectionToResult);
            }

            BlittableJsonReaderObject GetAggregatedValues()
            {
                foreach (var it in reader.SegmentsOrValues())
                {
                    if (it.IndividualValues != null)
                    {
                        AggregateIndividualItems(it.IndividualValues);
                    }
                    else
                    {
                        //We might need to close the old aggregation range and start a new one
                        MaybeMoveToNextRange(it.Segment.Start);

                        // now we need to see if we can consume the whole segment, or
                        // if the range it cover needs to be broken up to multiple ranges.
                        // For example, if the segment covers 3 days, but we have group by 1 hour,
                        // we still have to deal with the individual values
                        if (it.Segment.End > rangeSpec.End || timeSeriesFunction.Where != null)
                        {
                            AggregateIndividualItems(it.Segment.Values);
                        }
                        else
                        {
                            var span = it.Segment.Summary.SegmentValues.Span;
                            for (int i = 0; i < aggStates.Length; i++)
                            {
                                aggStates[i].Segment(span, reader.IsRaw);
                            }
                        }
                    }
                }

                if (aggStates[0].Any)
                {
                    array.Add(AddTimeSeriesResult(timeSeriesFunction, aggStates, rangeSpec.Start, rangeSpec.End));
                }

                _argumentValuesDictionary?.Clear();

                return GetFinalResult(array, aggStates[0].TotalCount, addProjectionToResult);
            }

            bool ShouldFilter(SingleResult singleResult, QueryExpression filter)
            {
                if (filter == null)
                    return false;

                if (filter is BinaryExpression be)
                {
                    switch (be.Operator)
                    {
                        case OperatorType.And:
                            return ShouldFilter(singleResult, be.Left) ||
                                   ShouldFilter(singleResult, be.Right);
                        case OperatorType.Or:
                            return ShouldFilter(singleResult, be.Left) &&
                                   ShouldFilter(singleResult, be.Right);
                    }

                    var left = GetValue(be.Left, singleResult);
                    var right = GetValue(be.Right, singleResult);
                    bool result;

                    try
                    {
                        switch (left)
                        {
                            case DateTime dt:
                                result = CompareDateTimes(dt, right);
                                break;
                            case double d:
                                result = CompareNumbers(d, right);
                                break;
                            case LazyNumberValue lnv:
                                result = CompareLazyNumbers(lnv, right);
                                break;
                            case string s:
                                result = CompareStrings(s, right);
                                break;
                            case LazyStringValue lsv:
                                result = CompareLazyStrings(lsv, right);
                                break;
                            default:
                                result = CompareDynamic(left, right);
                                break;
                        }

                        return result == false;
                    }
                    catch (Exception e)
                    {
                        throw new InvalidQueryException($"Time series function '{declaredFunction.Name}' failed to execute expression '{be}', got : left '{left}', right '{right}'", e);
                    }
                }

                if (filter is NegatedExpression ne)
                {
                    return ShouldFilter(singleResult, ne.Expression) == false;
                }

                if (filter is InExpression inExpression)
                {
                    var result = EvaluateInExpression();
                    return result == false;
                }

                if (filter is TimeSeriesBetweenExpression betweenExpression)
                {
                    var result = EvaluateBetweenExpression();
                    return result == false;
                }

                throw new InvalidQueryException($"Unsupported expression '{filter}' inside WHERE clause of TimeSeries function '{declaredFunction.Name}'. " +
                                                "Supported expressions are : Binary Expressions (=, !=, <, >, <=, >=, AND, OR, NOT), IN expressions, BETWEEN expressions");

                bool CompareNumbers(double d, object right)
                {
                    if (right is LazyNumberValue lnv)
                    {
                        var result = CompareLazyNumbers(lnv, d);
                        if (be.Operator == OperatorType.Equal || be.Operator == OperatorType.NotEqual)
                            return result;

                        return result == false;
                    }

                    double rightAsDouble;
                    if (right is double rd)
                    {
                        rightAsDouble = rd;
                    }
                    else if (right is long l)
                    {
                        rightAsDouble = l;
                    }
                    else
                    {
                        return CompareDynamic(d, right);
                    }

                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            return d.Equals(rightAsDouble);
                        case OperatorType.NotEqual:
                            return d.Equals(rightAsDouble) == false;
                        case OperatorType.LessThan:
                            return d.CompareTo(rightAsDouble) < 0;
                        case OperatorType.GreaterThan:
                            return d.CompareTo(rightAsDouble) > 0;
                        case OperatorType.LessThanEqual:
                            return d.CompareTo(rightAsDouble) <= 0;
                        case OperatorType.GreaterThanEqual:
                            return d.CompareTo(rightAsDouble) >= 0;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function." +
                                                            $"Operator '{be.Operator}' is not supported");
                    }
                }

                bool CompareLazyNumbers(LazyNumberValue lnv, object right)
                {
                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            return lnv.Equals(right);
                        case OperatorType.NotEqual:
                            return lnv.Equals(right) == false;
                        case OperatorType.LessThan:
                            return lnv.CompareTo(right) < 0;
                        case OperatorType.GreaterThan:
                            return lnv.CompareTo(right) > 0;
                        case OperatorType.LessThanEqual:
                            return lnv.CompareTo(right) <= 0;
                        case OperatorType.GreaterThanEqual:
                            return lnv.CompareTo(right) >= 0;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function." +
                                                            $"Operator '{be.Operator}' is not supported");
                    }
                }

                bool CompareDateTimes(DateTime? dateTime, object right)
                {
                    DateTime? rightAsDt;

                    if (right is DateTime dt)
                        rightAsDt = dt;
                    else
                        rightAsDt = ParseDateTime(right?.ToString());

                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            return dateTime.Equals(rightAsDt);
                        case OperatorType.NotEqual:
                            return dateTime.Equals(rightAsDt) == false;
                        case OperatorType.LessThan:
                            return dateTime < rightAsDt;
                        case OperatorType.GreaterThan:
                            return dateTime > rightAsDt;
                        case OperatorType.LessThanEqual:
                            return dateTime <= rightAsDt;
                        case OperatorType.GreaterThanEqual:
                            return dateTime >= rightAsDt;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function." +
                                                            $"Operator '{be.Operator}' is not supported");
                    }
                }

                bool CompareLazyStrings(LazyStringValue lsv, object right)
                {
                    if (right is DateTime)
                    {
                        var leftAsDt = ParseDateTime(lsv);
                        return CompareDateTimes(leftAsDt, right);
                    }

                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            return lsv.Equals(right);
                        case OperatorType.NotEqual:
                            return lsv.Equals(right) == false;
                        case OperatorType.LessThan:
                            return lsv.CompareTo(right) < 0;
                        case OperatorType.GreaterThan:
                            return lsv.CompareTo(right) > 0;
                        case OperatorType.LessThanEqual:
                            return lsv.CompareTo(right) <= 0;
                        case OperatorType.GreaterThanEqual:
                            return lsv.CompareTo(right) >= 0;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function." +
                                                            $"Operator '{be.Operator}' is not supported");
                    }
                }

                bool CompareStrings(string s, object right)
                {
                    if (right is DateTime)
                    {
                        var leftAsDt = ParseDateTime(s);
                        return CompareDateTimes(leftAsDt, right);
                    }

                    string rightAsString;
                    if (right is string rs)
                        rightAsString = rs;
                    else
                        rightAsString = right?.ToString();

                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            return s == rightAsString;
                        case OperatorType.NotEqual:
                            return s != rightAsString;
                        case OperatorType.LessThan:
                            return string.Compare(s, rightAsString, StringComparison.Ordinal) < 0;
                        case OperatorType.GreaterThan:
                            return string.Compare(s, rightAsString, StringComparison.Ordinal) > 0;
                        case OperatorType.LessThanEqual:
                            return string.Compare(s, rightAsString, StringComparison.Ordinal) <= 0;
                        case OperatorType.GreaterThanEqual:
                            return string.Compare(s, rightAsString, StringComparison.Ordinal) >= 0;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function." +
                                                            $"Operator '{be.Operator}' is not supported");
                    }
                }

                bool CompareDynamic(dynamic left, dynamic right)
                {
                    bool result;
                    switch (be.Operator)
                    {
                        case OperatorType.Equal:
                            result = Equals(left, right);
                            break;
                        case OperatorType.NotEqual:
                            result = Equals(left, right) == false;
                            break;
                        case OperatorType.LessThan:
                            result = left < right;
                            break;
                        case OperatorType.GreaterThan:
                            result = left > right;
                            break;
                        case OperatorType.LessThanEqual:
                            result = left <= right;
                            break;
                        case OperatorType.GreaterThanEqual:
                            result = left >= right;
                            break;
                        default:
                            throw new InvalidQueryException($"Invalid binary expression '{be}' inside WHERE clause of time series function '{declaredFunction.Name}'." +
                                                            $"Operator '{be.Operator}' is not supported");
                    }

                    return result;
                }

                bool EvaluateInExpression()
                {
                    dynamic src = GetValue(inExpression.Source, singleResult);
                    bool result = false;
                    dynamic val = null;

                    if (inExpression.All)
                        throw new InvalidQueryException($"Invalid InExpression '{inExpression}' inside WHERE clause of time series function '{declaredFunction.Name}'." +
                                                        "Operator 'ALL IN' is not supported for time series functions");
                    try
                    {
                        if (inExpression.Values.Count == 1 &&
                            inExpression.Values[0] is ValueExpression valueExpression &&
                            valueExpression.Value == ValueTokenType.Parameter)
                        {
                            var values = (IEnumerable)GetValue(valueExpression, singleResult);

                            foreach (var v in values)
                            {
                                if (src is LazyNumberValue lnv)
                                {
                                    if (lnv.CompareTo(v) == 0)
                                    {
                                        result = true;
                                        break;
                                    }
                                }
                                else if (v is LazyStringValue lsv)
                                {
                                    if (lsv.Equals(src))
                                    {
                                        result = true;
                                        break;
                                    }
                                }
                                else if (src == v)
                                {
                                    result = true;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            for (int i = 0; i < inExpression.Values.Count; i++)
                            {
                                val = GetValue(inExpression.Values[i], singleResult);

                                if (src is LazyNumberValue lnv)
                                {
                                    if (lnv.CompareTo(val) == 0)
                                    {
                                        result = true;
                                        break;
                                    }
                                }
                                else if (src == val)
                                {
                                    result = true;
                                    break;
                                }
                            }
                        }

                        return result;
                    }
                    catch (Exception e)
                    {
                        throw new InvalidQueryException($"Time series function '{declaredFunction.Name}' failed to execute InExpression '{inExpression}' on : source '{src}', value '{val}'", e);
                    }
                }

                bool EvaluateBetweenExpression()
                {
                    var result = false;

                    dynamic src = GetValue(betweenExpression.Source, singleResult);
                    dynamic value = GetValue(betweenExpression.MinExpression, singleResult);

                    try
                    {
                        if (src is LazyNumberValue lnv)
                        {
                            if (lnv.CompareTo(value) >= 0)
                            {
                                value = GetValue(betweenExpression.MaxExpression, singleResult);
                                result = lnv.CompareTo(value) <= 0;
                            }
                        }
                        else if (src >= value)
                        {
                            value = GetValue(betweenExpression.MaxExpression, singleResult);
                            result = src <= value;
                        }

                        return result;
                    }
                    catch (Exception e)
                    {
                        throw new InvalidQueryException($"Time series function '{declaredFunction.Name}' failed to execute BetweenExpression '{betweenExpression}'", e);
                    }
                }
            }

            object GetValue(QueryExpression expression, SingleResult singleResult)
            {
                if (expression is FieldExpression fe)
                {
                    switch (fe.Compound[0].Value)
                    {
                        case "TAG":
                        case "Tag":
                        case "tag":
                            if (fe.Compound.Count > 1)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");
                            return singleResult.Tag?.ToString();
                        case "VALUES":
                        case "Values":
                        case "values":
                            if (fe.Compound.Count == 1)
                                return singleResult.Values;

                            if (fe.Compound.Count > 2)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");

                            if (int.TryParse(fe.Compound[1].Value, out var index) == false)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");

                            if (index >= singleResult.Values.Length)
                                return null;
                            if (reader.IsRaw)
                                return singleResult.Values.Span[index];

                            return GetValueFromRolledUpEntry(index, singleResult);

                        case "VALUE":
                        case "Value":
                        case "value":
                            if (fe.Compound.Count > 1)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");
                            if (reader.IsRaw)
                                return singleResult.Values.Span[0];

                            // rolled-up series
                            if ((int)AggregationType.Count >= singleResult.Values.Length)
                                return null;
                            if (singleResult.Values.Span[(int)AggregationType.Count] == 0)
                                return double.NaN;
                            return singleResult.Values.Span[(int)AggregationType.Sum] / singleResult.Values.Span[(int)AggregationType.Count];

                        case "TIMESTAMP":
                        case "TimeStamp":
                        case "Timestamp":
                        case "timestamp":
                            if (fe.Compound.Count > 1)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");
                            return singleResult.Timestamp;
                        default:
                            if (fe.Compound[0].Value == timeSeriesFunction.LoadTagAs?.Value)
                                return GetValueFromLoadedTag(fe, singleResult);

                            var val = GetNamedValue(fe.Compound[0].Value, singleResult, reader.IsRaw);
                            if (val != null)
                                return val;

                            if (_argumentValuesDictionary.TryGetValue(fe, out val) == false)
                                _argumentValuesDictionary[fe] = val = GetValueFromArgument(declaredFunction, args, fe);

                            return val;
                    }
                }

                if (expression is ValueExpression ve)
                {
                    if (_valuesDictionary.TryGetValue(ve, out var val) == false)
                    {
                        _valuesDictionary[ve] = val = ve.Value == ValueTokenType.String
                            ? ve.Token.Value
                            : ve.GetValue(_queryParameters);
                    }

                    return val;
                }

                // shouldn't happen - query parser should have caught this
                throw new InvalidQueryException($"Failed to invoke time series function '{declaredFunction.Name}'. Unable to get the value of expression '{expression}'. " +
                                                    $"Unsupported expression type : '{expression.Type}'");
            }

            string GetSourceAndId()
            {
                if (timeSeriesFunction.Source is ValueExpression valueExpression)
                {
                    var val = valueExpression.Value == ValueTokenType.String
                        ? valueExpression.Token.Value
                        : valueExpression.GetValue(_queryParameters);

                    if (!(val is string || val is LazyStringValue))
                        throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{timeSeriesFunction.Source}'. " +
                                                        $"Expected argument '{val}' to be a string, but got '{val.GetType()}'");
                    return val.ToString();
                }

                if (!(timeSeriesFunction.Source is FieldExpression field))
                    throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{timeSeriesFunction.Source}'. " +
                                                    $"Expected time series name to be a ValueExpression or a FieldExpression, but got '{timeSeriesFunction.Source.GetType()}'");
            
                var compound = field.Compound;

                if (compound.Count == 1)
                {
                    var paramIndex = GetParameterIndex(declaredFunction, compound[0]);
                    if (paramIndex == -1 || paramIndex == declaredFunction.Parameters.Count) 
                        return field.FieldValue; //not found

                    if (!(args[paramIndex] is string || args[paramIndex] is LazyStringValue))
                        throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{timeSeriesFunction.Source}'. " +
                                                        $"Expected argument '{compound[0]}' to be a string, but got '{args[paramIndex].GetType()}'");
                    return args[paramIndex].ToString();
                }

                if (args == null)
                    throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{timeSeriesFunction.Source}'. " +
                                                        $"'{compound[0]}' is unknown, and no arguments were provided to time series function '{declaredFunction.Name}'.");

                if (args.Length < declaredFunction.Parameters.Count)
                    throw new InvalidQueryException($"Incorrect number of arguments passed to time series function '{declaredFunction.Name}'." +
                                                        $"Expected '{declaredFunction.Parameters.Count}' arguments, but got '{args.Length}'");

                var index = GetParameterIndex(declaredFunction, compound[0]);
                if (index == 0)
                {
                    if (args[0] is Document document)
                        documentId = document.Id;
                }
                else
                {
                    if (index == -1 || index == declaredFunction.Parameters.Count) // not found
                        throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{timeSeriesFunction.Source}'. " +
                                                            $"'{compound[0]}' is unknown, and no matching argument was provided to time series function '{declaredFunction.Name}'.");

                    if (!(args[index] is Document document))
                        throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{timeSeriesFunction.Source}'. " +
                                                            $"Expected argument '{compound[0]}' to be a Document instance, but got '{args[index].GetType()}'");

                    documentId = document.Id;
                }

                return field.FieldValueWithoutAlias;
            }
        }

        private static object GetValueFromRolledUpEntry(int index, SingleResult singleResult)
        {
            // we are working with a rolled-up series
            // here an entry has 6 different values (min, max, first, last, sum, count) per each 'original' measurement
            // we need to return the average value (sum / count)

            index *= 6;
            if (index + (int)AggregationType.Count >= singleResult.Values.Length)
                return null;
            if (singleResult.Values.Span[index + (int)AggregationType.Count] == 0)
                return double.NaN;
            return singleResult.Values.Span[index + (int)AggregationType.Sum] / singleResult.Values.Span[index + (int)AggregationType.Count];
        }

        private object GetNamedValue(string fieldValue, SingleResult singleResult, bool isRaw)
        {
            if (_configurationFetched == false)
            {
                var config = _context.DocumentDatabase.ServerStore.Cluster.ReadTimeSeriesConfiguration(_context.DocumentDatabase.Name);
                _namedValues = config?.GetNames(_collection, _source);
                _configurationFetched = true;
            }

            if (_namedValues == null)
                return null;

            int index;
            for (index = 0; index < _namedValues.Length; index++)
            {
                if (_namedValues[index] == fieldValue)
                    break;
            }

            if (index == _namedValues.Length ||
                index >= singleResult.Values.Length) // shouldn't happen
                return null;

            if (isRaw)
                return singleResult.Values.Span[index];

            return GetValueFromRolledUpEntry(index, singleResult);
        }

        private BlittableJsonReaderObject GetFinalResult(DynamicJsonArray array, long count, bool addProjectionToResult)
        {
            var result = new DynamicJsonValue
            {
                [nameof(TimeSeriesQueryResult.Count)] = count,
                [nameof(TimeSeriesAggregationResult.Results)] = array
            };

            if (addProjectionToResult)
            {
                result[Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Projection] = true
                };
            }

            AddNamesIfNeeded(result);

            return _context.ReadObject(result, "timeseries/value");
        }

        private void AddNamesIfNeeded(DynamicJsonValue result)
        {
            if (_isFromStudio == false) 
                return;

            var metadata = (DynamicJsonValue)(result[Constants.Documents.Metadata.Key] ?? (result[Constants.Documents.Metadata.Key] = new DynamicJsonValue()));
            if (_configurationFetched == false)
            {
                var config = _context.DocumentDatabase.ServerStore.Cluster.ReadTimeSeriesConfiguration(_context.DocumentDatabase.Name);
                _namedValues = config?.GetNames(_collection, _source);
            }

            if (_namedValues != null)
            {
                metadata[Constants.Documents.Metadata.TimeSeriesNamedValues] = new DynamicJsonArray(_namedValues);
            }
        }

        private (DateTime From, DateTime To) GetFromAndTo(DeclaredFunction declaredFunction, string documentId, object[] args, TimeSeriesFunction timeSeriesFunction, TimeSpan? offset)
        {
            DateTime from, to;
            if (timeSeriesFunction.Last != null)
            {
                to = DateTime.MaxValue;

                var timeFromLast = GetTimePeriodFromValueExpression(timeSeriesFunction.Last, nameof(TimeSeriesFunction.Last), declaredFunction.Name, documentId);
                from = _stats.End.Add(-timeFromLast);
            }
            else if (timeSeriesFunction.First != null)
            {
                from = DateTime.MinValue;
                
                var timeFromFirst = GetTimePeriodFromValueExpression(timeSeriesFunction.First, nameof(TimeSeriesFunction.First), declaredFunction.Name, documentId);
                to = _stats.Start.Add(timeFromFirst);
            }
            else
            {
                from = GetDateValue(timeSeriesFunction.Between?.MinExpression, declaredFunction, args) ?? DateTime.MinValue;
                to = GetDateValue(timeSeriesFunction.Between?.MaxExpression, declaredFunction, args) ?? DateTime.MaxValue;
            }

            if (offset.HasValue)
            {
                var minWithOffset = from.Ticks + offset.Value.Ticks;
                var maxWithOffset = to.Ticks + offset.Value.Ticks;

                if (minWithOffset >= 0 && minWithOffset <= DateTime.MaxValue.Ticks)
                    from = from.Add(offset.Value);
                if (maxWithOffset >= 0 && maxWithOffset <= DateTime.MaxValue.Ticks)
                    to = to.Add(offset.Value);

            }

            return (from, to);
        }

        private TimeValue GetTimePeriodFromValueExpression(ValueExpression valueExpression, string methodName, string functionName, string documentId)
        {
            var timePeriod = valueExpression.GetValue(_queryParameters)?.ToString();
            if (timePeriod == null)
            {
                throw new InvalidQueryException($"Time series function '{functionName}' on document '{documentId}' " +
                                                $"was unable to read '{methodName}' expression '{valueExpression}'");
            }

            return RangeGroup.ParseTimePeriodFromString(timePeriod);
        }

        private TimeSpan? GetOffset(ValueExpression offsetExpression, string name)
        {
            if (offsetExpression == null)
                return null;

            var val = offsetExpression.Value == ValueTokenType.String
                ? offsetExpression.Token.Value
                : offsetExpression.GetValue(_queryParameters);

            if (val == null)
                throw new InvalidOperationException("Unable to parse time series offset. Got null instead of a value");

            if (!(val is LazyStringValue) && !(val is string) ||
                TimeSpan.TryParse(val.ToString(), out var timeSpan) == false)
                throw new InvalidOperationException($"Failed to parse object '{val}' as TimeSpan, in OFFSET clause of time series function '{name}'");

            return timeSpan;
        }

        private string GetCollection(string documentId)
        {
            _loadedDocuments ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);

            if (_loadedDocuments.TryGetValue(documentId, out var doc) == false)
            {
                _loadedDocuments[documentId] = doc = _context.DocumentDatabase.DocumentsStorage.Get(_context, documentId);
            }

            return CollectionName.GetCollectionName(doc?.Data);
        }

        private static object GetValueFromArgument(DeclaredFunction declaredFunction, object[] args, FieldExpression fe)
        {
            if (args == null || declaredFunction.Parameters == null)
                throw new InvalidQueryException($"Unable to get the value of '{fe}'. '{fe.Compound[0]}' is unknown, and no arguments were provided to time series function '{declaredFunction.Name}'.");

            if (args.Length < declaredFunction.Parameters.Count)
                throw new InvalidQueryException($"Incorrect number of arguments passed to time series function '{declaredFunction.Name}'. " +
                                                    $"Expected '{declaredFunction.Parameters.Count}', but got '{args.Length}.'");

            var index = GetParameterIndex(declaredFunction, fe.Compound[0]);

            if (index == declaredFunction.Parameters.Count) // not found
                throw new InvalidQueryException($"Unable to get the value of '{fe}'. '{fe.Compound[0]}' is unknown, " +
                                                    $"and no matching argument was provided to time series function '{declaredFunction.Name}'.");

            if (args[index] == null)
                return null;

            if (!(args[index] is Document doc))
            {
                if (index == 0 && args[0] is Tuple<Document, Lucene.Net.Documents.Document, IState, Dictionary<string, IndexField>, bool?> tuple)
                    doc = tuple.Item1;
                else
                    return args[index];
            }

            if (fe.Compound.Count == 1)
                return doc;

            return GetFieldFromDocument(fe, doc);
        }

        private static int GetParameterIndex(DeclaredFunction declaredFunction, StringSegment str)
        {
            if (declaredFunction.Parameters == null)
                return -1;

            for (int i = 0; i < declaredFunction.Parameters.Count; i++)
            {
                var parameter = declaredFunction.Parameters[i];

                if (str == ((FieldExpression)parameter)?.FieldValue)
                    return i;
            }

            return declaredFunction.Parameters.Count;
        }

        private static object GetFieldFromDocument(FieldExpression fe, Document document)
        {
            if (document == null)
                return null;

            var currentPart = 1;
            object val = null;
            var data = document.Data;

            while (currentPart < fe.Compound.Count)
            {
                if (data.TryGetMember(fe.Compound[currentPart], out val) == false)
                {
                    var id = currentPart == 1
                        ? document.Id
                        : data.TryGetId(out var nestedId)
                            ? nestedId : null;

                    throw new InvalidQueryException($"Unable to get the value of '{fe.FieldValueWithoutAlias}' from document '{document.Id}'. " +
                                                        $"Document '{id}' does not have a property named '{fe.Compound[currentPart]}'.");
                }

                if (!(val is BlittableJsonReaderObject nested))
                    break;

                data = nested;
                currentPart++;
            }

            return val;
        }

        private object GetValueFromLoadedTag(FieldExpression fe, SingleResult singleResult)
        {
            if (_loadedDocuments == null)
                _loadedDocuments = new Dictionary<string, Document>();

            var tag = singleResult.Tag?.ToString();
            if (tag == null)
                return null;

            if (_loadedDocuments.TryGetValue(tag, out var document) == false)
                _loadedDocuments[tag] = document = _context.DocumentDatabase.DocumentsStorage.Get(_context, tag);

            if (fe.Compound.Count == 1)
                return document;

            return GetFieldFromDocument(fe, document);
        }

        private static void InitializeAggregationStates(TimeSeriesFunction timeSeriesFunction, TimeSeriesAggregation[] aggStates)
        {
            for (int i = 0; i < timeSeriesFunction.Select.Count; i++)
            {
                if (timeSeriesFunction.Select[i].QueryExpression is MethodExpression me)
                {
                    if (Enum.TryParse(me.Name.Value, ignoreCase: true, out AggregationType type))
                    {
                        aggStates[i] = new TimeSeriesAggregation(type);
                        continue;
                    }

                    if (me.Name.Value == "avg")
                    {
                        aggStates[i] = new TimeSeriesAggregation(AggregationType.Average);
                        continue;
                    }

                    throw new ArgumentException("Unknown method in timeseries query: " + me);
                }

                throw new ArgumentException("Unknown method in timeseries query: " + timeSeriesFunction.Select[i].QueryExpression);
            }
        }

        private static DynamicJsonValue AddTimeSeriesResult(TimeSeriesFunction func, TimeSeriesAggregation[] aggStates, DateTime start, DateTime next)
        {
            DateTime? from = start, to = next;
            if (start == DateTime.MinValue)
                from = null;
            if (next == DateTime.MaxValue)
                to = null;

            var result = new DynamicJsonValue
            {
                [nameof(TimeSeriesRangeAggregation.From)] = from,
                [nameof(TimeSeriesRangeAggregation.To)] = to,
                [nameof(TimeSeriesRangeAggregation.Count)] = new DynamicJsonArray(aggStates[0].Count)
            };

            for (int i = 0; i < aggStates.Length; i++)
            {
                var name = func.Select?[i].StringSegment?.ToString() ?? aggStates[i].Aggregation.ToString();
                result[name] = new DynamicJsonArray(aggStates[i].GetFinalValues());
            }

            return result;
        }

        private DateTime? GetDateValue(QueryExpression qe, DeclaredFunction func, object[] args)
        {
            if (qe == null)
                return null;

            if (qe is ValueExpression ve)
            {
                if (_valuesDictionary.TryGetValue(ve, out var value))
                    return (DateTime)value;

                var val = ve.GetValue(_queryParameters);
                if (val == null)
                    throw new ArgumentException("Unable to parse timeseries from/to values. Got a null instead of a value");

                DateTime? result;
                _valuesDictionary[ve] = result = ParseDateTime(val.ToString());

                return result;
            }

            if (qe is FieldExpression fe)
            {
                var val = GetValueFromArgument(func, args, fe);
                return ParseDateTime(val.ToString());
            }

            throw new ArgumentException("Unable to parse timeseries from/to values. Got: " + qe);
        }

        public static DateTime ParseDateTime(string valueAsStr)
        {
            if (DateTime.TryParseExact(valueAsStr, SupportedDateTimeFormats, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var date) == false)
                throw new ArgumentException($"Unable to parse timeseries from/to values. Got: {valueAsStr}{Environment.NewLine}" +
                                            $"The supported time formats are:{Environment.NewLine}" +
                                            $"{string.Join(Environment.NewLine, SupportedDateTimeFormats.OrderBy(f => f.Length))}");
            return DateTime.SpecifyKind(date, DateTimeKind.Utc);
        }

        private static readonly string[] SupportedDateTimeFormats =
        {
            "yyyy-MM-ddTHH:mm:ss.fffffffZ",
            "yyyy-MM-ddTHH:mm:ss.fffffff",
            "yyyy-MM-ddTHH:mm:ss",
            "yyyy-MM-dd",
            "yyyy",
            "yyyy-MM",
            "yyyy-MM-ddTHH:mm",
            "yyyy-MM-ddTHH:mm:ss.fffZ",
            "yyyy-MM-ddTHH:mm:ss.fff"
        };
    }
}
