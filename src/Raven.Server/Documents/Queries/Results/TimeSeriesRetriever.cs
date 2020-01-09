using System;
using System.Collections;
using Sparrow.Json.Parsing;
using Sparrow.Json;
using System.Collections.Generic;
using Lucene.Net.Store;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.Extensions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;
using static Raven.Server.Documents.TimeSeries.TimeSeriesStorage.Reader;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.TimeSeries;
using Sparrow;
using BinaryExpression = Raven.Server.Documents.Queries.AST.BinaryExpression;

namespace Raven.Server.Documents.Queries.Results
{
    public class TimeSeriesRetriever
    {
        private readonly Dictionary<ValueExpression, object> _valuesDictionary;

        private readonly Dictionary<FieldExpression, object> _argumentValuesDictionary;

        private readonly BlittableJsonReaderObject _queryParameters;

        private readonly DocumentsOperationContext _context;

        private Dictionary<string, Document> _loadedDocuments;

        private readonly DocumentDatabase _database;

        public TimeSeriesRetriever(DocumentDatabase database, DocumentsOperationContext context, 
            BlittableJsonReaderObject queryParameters, Dictionary<string, Document> loadedDocuments)
        {
            _database = database;
            _context = context;
            _queryParameters = queryParameters;
            _loadedDocuments = loadedDocuments;

            _valuesDictionary = new Dictionary<ValueExpression, object>();
            _argumentValuesDictionary = new Dictionary<FieldExpression, object>();
        }


        public BlittableJsonReaderObject InvokeTimeSeriesFunction(DeclaredFunction declaredFunction, string documentId, object[] args)
        {
            var tss = _database.DocumentsStorage.TimeSeriesStorage;
            var timeSeriesFunction = declaredFunction.TimeSeries;
            var source = GetSourceAndId();

            var min = GetDateValue(timeSeriesFunction.Between.MinExpression, declaredFunction, args) ?? DateTime.MinValue;
            var max = GetDateValue(timeSeriesFunction.Between.MaxExpression, declaredFunction, args) ?? DateTime.MaxValue;

            long count = 0;
            var array = new DynamicJsonArray();
            TimeSeriesStorage.Reader reader;

            if (timeSeriesFunction.GroupBy == null)
                return GetRawValues();
            
            var groupBy = timeSeriesFunction.GroupBy.GetValue(_queryParameters)?.ToString();
            if (groupBy == null)
                throw new ArgumentException("Unable to parse group by value, expected range specification, but got a null");

            var rangeSpec = TimeSeriesFunction.ParseRangeFromString(groupBy);

            var aggStates = new TimeSeriesAggregation[timeSeriesFunction.Select.Count];
            InitializeAggregationStates(timeSeriesFunction, aggStates);

            DateTime start = default, next = default;
            return GetAggregatedValues();

            void AggregateIndividualItems(IEnumerable<SingleResult> items)
            {
                foreach (var cur in items)
                {
                    MaybeMoveToNextRange(cur.Timestamp);

                    if (ShouldFilter(cur, timeSeriesFunction.Where))
                        continue;
                    
                    count++;
                    for (int i = 0; i < aggStates.Length; i++)
                    {
                        aggStates[i].Step(cur.Values.Span);
                    }
                }
            }

            void MaybeMoveToNextRange(DateTime ts)
            {
                if (ts <= next)
                    return;

                if (aggStates[0].Any)
                {
                    array.Add(AddTimeSeriesResult(timeSeriesFunction, aggStates, start, next));
                }

                start = rangeSpec.GetRangeStart(ts);
                next = rangeSpec.GetNextRangeStart(start);

                for (int i = 0; i < aggStates.Length; i++)
                {
                    aggStates[i].Init();
                }
            }

            BlittableJsonReaderObject GetRawValues()
            {
                reader = tss.GetReader(_context, documentId, source, min, max);

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
                        [nameof(TimeSeriesEntry.Tag)] = singleResult.Tag.ToString(),
                        [nameof(TimeSeriesEntry.Timestamp)] = singleResult.Timestamp,
                        [nameof(TimeSeriesEntry.Values)] = vals,
                        [nameof(TimeSeriesEntry.Value)] = singleResult.Values.Span[0]
                    });

                    count++;
                }

                _argumentValuesDictionary.Clear();

                return _context.ReadObject(new DynamicJsonValue
                {
                    ["Count"] = count,
                    ["Results"] = array
                }, "timeseries/value");
            }

            BlittableJsonReaderObject GetAggregatedValues()
            {
                reader = tss.GetReader(_context, documentId, source, min, max);

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
                        if (it.Segment.End > next || timeSeriesFunction.Where != null)
                        {
                            AggregateIndividualItems(it.Segment.Values);
                        }
                        else
                        {
                            var span = it.Segment.Summary.Span;
                            for (int i = 0; i < aggStates.Length; i++)
                            {
                                aggStates[i].Segment(span);
                            }

                            count += span[0].Count;
                        }
                    }
                }

                if (aggStates[0].Any)
                {
                    array.Add(AddTimeSeriesResult(timeSeriesFunction, aggStates, start, next));
                }

                _argumentValuesDictionary?.Clear();

                return _context.ReadObject(new DynamicJsonValue
                {
                    ["Count"] = count,
                    ["Results"] = array
                }, "timeseries/value");
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
                        rightAsString = right.ToString();
                    
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

                            return singleResult.Values.Span[index];
                        case "VALUE":
                        case "Value":
                        case "value":
                            if (fe.Compound.Count > 1)
                                throw new InvalidQueryException($"Failed to evaluate expression '{fe}'");

                            return singleResult.Values.Span[0];
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
                            
                            if (_argumentValuesDictionary.TryGetValue(fe, out var val) == false)
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
                var compound = ((FieldExpression)timeSeriesFunction.Between.Source).Compound;

                if (compound.Count == 1)
                {
                    var paramIndex = GetParameterIndex(declaredFunction, compound[0]);
                    if (paramIndex == -1 || paramIndex == declaredFunction.Parameters.Count) //not found
                        return ((FieldExpression)timeSeriesFunction.Between.Source).FieldValue;
                    if (!(args[paramIndex] is string s))
                        throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{(FieldExpression)timeSeriesFunction.Between.Source}'. " +
                                                        $"Expected argument '{compound[0]}' to be a Document instance, but got '{args[paramIndex].GetType()}'"); //todo aviv : write a better error message
                    return s;
                }

                if (args == null)
                    throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{(FieldExpression)timeSeriesFunction.Between.Source}'. " +
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
                        throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{(FieldExpression)timeSeriesFunction.Between.Source}'. " +
                                                            $"'{compound[0]}' is unknown, and no matching argument was provided to time series function '{declaredFunction.Name}'.");
                    
                    if (!(args[index] is Document document))
                        throw new InvalidQueryException($"Unable to parse TimeSeries name from expression '{(FieldExpression)timeSeriesFunction.Between.Source}'. " +
                                                            $"Expected argument '{compound[0]}' to be a Document instance, but got '{args[index].GetType()}'");
                    
                    documentId = document.Id;
                }

                return ((FieldExpression)timeSeriesFunction.Between.Source).FieldValueWithoutAlias;
            }
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
                _loadedDocuments[tag] = document = _database.DocumentsStorage.Get(_context, tag);
            
            if (fe.Compound.Count == 1)
                return document;

            return GetFieldFromDocument(fe, document);
        }

        private static void InitializeAggregationStates(TimeSeriesFunction timeSeriesFunction, TimeSeriesAggregation[] aggStates)
        {
            for (int i = 0; i < timeSeriesFunction.Select.Count; i++)
            {
                if (timeSeriesFunction.Select[i].Item1 is MethodExpression me)
                {
                    if (Enum.TryParse(me.Name.Value, ignoreCase: true, out TimeSeriesAggregation.Type type))
                    {
                        aggStates[i] = new TimeSeriesAggregation(type);
                        continue;
                    }

                    throw new ArgumentException("Unknown method in timeseries query: " + me);
                }

                throw new ArgumentException("Unknown method in timeseries query: " + timeSeriesFunction.Select[i].Item1);
            }
        }

        private static DynamicJsonValue AddTimeSeriesResult(TimeSeriesFunction func, TimeSeriesAggregation[] aggStates, DateTime start, DateTime next)
        {
            var result = new DynamicJsonValue
            {
                ["From"] = start,
                ["To"] = next,
                ["Count"] = new DynamicJsonArray(aggStates[0].Count)
            };
            for (int i = 0; i < aggStates.Length; i++)
            {
                var name = func.Select[i].Item2?.ToString() ?? aggStates[i].Aggregation.ToString();
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

        private static unsafe DateTime? ParseDateTime(string valueAsStr)
        {
            fixed (char* c = valueAsStr)
            {
                var result = LazyStringParser.TryParseDateTime(c, valueAsStr.Length, out var dt, out _);
                if (result != LazyStringParser.Result.DateTime)
                    throw new ArgumentException("Unable to parse timeseries from/to values. Got: " + valueAsStr);
                return dt;
            }
        }
    }
}
