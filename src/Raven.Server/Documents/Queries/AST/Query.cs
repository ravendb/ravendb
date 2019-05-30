using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Exceptions;
using Sparrow;
using Raven.Server.Utils;
using Raven.Server.Documents.TimeSeries;

namespace Raven.Server.Documents.Queries.AST
{
    public class Query
    {
        public bool IsDistinct;
        public GraphQuery GraphQuery;
        public QueryExpression Where;
        public FromClause From;
        public List<(QueryExpression Expression, StringSegment? Alias)> Select;
        public List<(QueryExpression Expression, StringSegment? Alias)> Load;
        public List<QueryExpression> Include;
        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;
        public List<(QueryExpression Expression, StringSegment? Alias)> GroupBy;

        public Dictionary<string, DeclaredFunction> DeclaredFunctions;

        public string QueryText;
        public (string FunctionText, Esprima.Ast.Program Program) SelectFunctionBody;
        public string UpdateBody;
        public ValueExpression Offset;
        public ValueExpression Limit;

        public bool TryAddFunction(DeclaredFunction func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<string, DeclaredFunction>(StringComparer.OrdinalIgnoreCase);

            return DeclaredFunctions.TryAdd(func.Name, func);
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            new StringQueryVisitor(sb).Visit(this);
            return sb.ToString();
        }

        public void TryAddWithClause(Query query, StringSegment alias, bool implicitAlias)
        {
            if (GraphQuery == null)
            {
                GraphQuery = new GraphQuery();                
            }

            if (GraphQuery.WithDocumentQueries.TryGetValue(alias, out var existing))
            {
                if (query.From.From.Compound.Count == 0)
                    return; // reusing an alias defined explicitly before

                if (existing.withQuery.From.From.Compound.Count == 0)
                {
                    // using an alias that is defined _later_ in the query
                    GraphQuery.WithDocumentQueries[alias] = (implicitAlias, query);
                    return;
                }

                throw new InvalidQueryException($"Alias {alias} is already in use on a different 'With' clause", QueryText);
            }

            GraphQuery.WithDocumentQueries.Add(alias, (implicitAlias, query));
        }

        public void TryAddWithEdgePredicates(WithEdgesExpression expr, StringSegment alias)
        {
            if (GraphQuery == null)
            {
                GraphQuery = new GraphQuery();               
            }

            if (GraphQuery.WithEdgePredicates.ContainsKey(alias))
            {
                if (expr.Path.Compound.Count == 0 && expr.OrderBy == null && expr.Where == null)
                    return;

                throw new InvalidQueryException($"Allias {alias} is already in use on a diffrent 'With' clause",
                    QueryText, null);
            }

            GraphQuery.WithEdgePredicates.Add(alias, expr);
        }
    }

    public class DeclaredFunction
    {
        public string Name;
        public string FunctionText;
        public Esprima.Ast.Program JavaScript;
        public TimeSeriesFunction TimeSeries;
        public FunctionType Type;

        public enum FunctionType
        {
            JavaScript,
            TimeSeries
        }
    }

    public unsafe class TimeSeriesFunction
    {
        public BetweenExpression Between;
        public ValueExpression GroupBy;
        public List<(QueryExpression, StringSegment?)> Select;


        public struct RangeGroup
        {
            public long Ticks;
            public int Months;

            public DateTime GetRangeStart(DateTime timestamp)
            {
                if (timestamp.Kind != DateTimeKind.Utc)
                    throw new ArgumentException("The timestamp must be in UTC");

                var ticks = timestamp.Ticks;

                if (Ticks != 0)
                {
                    ticks -= (ticks % Ticks);
                    return new DateTime(ticks,timestamp.Kind);
                }

                if (Months != 0)
                {
                    var yearsPortion = Math.Max(1, Months / 12);
                    var monthsRemaining = Months % 12;
                    var year = timestamp.Year - (timestamp.Year % yearsPortion);
                    int month = monthsRemaining == 0 ? 1 : ((timestamp.Month - 1) / monthsRemaining * monthsRemaining) + 1;

                    return new DateTime(year, month, 1, 0, 0, 0, timestamp.Kind);
                }
                return timestamp;
            }

            public DateTime GetNextRangeStart(DateTime timestamp)
            {
                if (timestamp.Kind != DateTimeKind.Utc)
                    throw new ArgumentException("The timestamp must be in UTC");

                if (Ticks != 0)
                {
                    return timestamp.AddTicks(Ticks);
                }

                if (Months != 0)
                {
                    return timestamp.AddMonths(Months);
                }
                return timestamp;
            }
        }

        public static RangeGroup ParseRangeFromString(string s)
        {
            var range = new RangeGroup();
            var offset = 0;

            var duration = ParseNumber(s, ref offset);
            ParseRange(s, ref offset, ref range, duration);

            while (offset < s.Length && char.IsWhiteSpace(s[offset]))
            {
                offset++;
            }
            if (offset != s.Length)
                throw new ArgumentException("After range specification, found additional unknown data: " + s);

            return range;
        }

        private static void ParseRange(string source, ref int offset, ref RangeGroup range, long duration)
        {
            if (offset >= source.Length)
                throw new ArgumentException("Unable to find range specification in: " + source);

            while (char.IsWhiteSpace(source[offset]) && offset < source.Length)
            {
                offset++;
            }

            if (offset >= source.Length)
                throw new ArgumentException("Unable to find range specification in: " + source);

            switch (char.ToLower(source[offset++]))
            {
                case 's':
                    if (TryConsumeMatch(source, ref offset, "seconds") == false)
                        TryConsumeMatch(source, ref offset, "second");

                    range.Ticks += duration * 10_000_000;
                    return;
                case 'm':
                    if (TryConsumeMatch(source, ref offset, "minutes") ||
                        TryConsumeMatch(source, ref offset, "min"))
                    {
                        range.Ticks += duration * 10_000_000 * 60;
                        return;
                    }

                    if (TryConsumeMatch(source, ref offset, "ms") ||
                        TryConsumeMatch(source, ref offset, "milli") ||
                        TryConsumeMatch(source, ref offset, "milliseconds"))
                    {
                        range.Ticks += duration;
                        return;
                    }
                    if (TryConsumeMatch(source, ref offset, "months") ||
                        TryConsumeMatch(source, ref offset, "month") ||
                        TryConsumeMatch(source, ref offset, "mon"))
                    {
                        AssertValidDurationInMonths(duration);
                        range.Months+= (int)duration;
                        return;
                    }
                    range.Ticks += duration * 10_000_000 * 60;
                    return;
                case 'h':
                    if (TryConsumeMatch(source, ref offset, "hours") == false)
                        TryConsumeMatch(source, ref offset, "hour");

                    range.Ticks += duration * 10_000_000 * 60 * 60;
                    return;
                case 'd':
                    if (TryConsumeMatch(source, ref offset, "days") == false)
                        TryConsumeMatch(source, ref offset, "day");
                    range.Ticks += duration * 10_000_000 * 60 * 60 * 24;
                    return;
                case 'q':
                    if (TryConsumeMatch(source, ref offset, "quarters") == false)
                        TryConsumeMatch(source, ref offset, "quarter");
                    duration *= 3;
                    AssertValidDurationInMonths(duration);
                    range.Months += (int)duration;
                    return;

                case 'y':
                    if (TryConsumeMatch(source, ref offset, "years") == false)
                        TryConsumeMatch(source, ref offset, "year");
                    duration *= 12;
                    AssertValidDurationInMonths(duration);
                    range.Months += (int)duration;
                    return;
                default:
                    throw new ArgumentException("Unable to understand time range: " + source.Substring(offset));
            }
        }

        private static void AssertValidDurationInMonths(long duration)
        {
            if (duration > 120_000)
                throw new ArgumentException("The specified range results in invalid range, cannoot have: " + duration + " months");
        }

        private static bool TryConsumeMatch(string source, ref int offset, string additionalMatch)
        {
            if (source.Length <= offset)
                return false;

            if (new StringSegment(source, offset-1 , source.Length - offset +1).StartsWith(additionalMatch, StringComparison.OrdinalIgnoreCase))
            {
                offset += additionalMatch.Length-1;
                return true;
            }
            return false;
        }

        private static long ParseNumber(string source, ref int offset)
        {
            int i;
            for (i= offset; i < source.Length; i++)
            {
                if (char.IsWhiteSpace(source[i]) == false)
                    break;
            }

            for (; i < source.Length; i++)
            {
                if (char.IsNumber(source[i]) == false)
                    break;
            }

            fixed(char* s = source)
            {
                if (long.TryParse(new ReadOnlySpan<char>(s + offset, i), out var amount) )
                {
                    offset = i;
                    return amount;
                }
            }

            throw new ArgumentException("Unable to parse: '" + source.Substring(offset) + "' as a number");
        }
    }


    public struct TimeSeriesAggregation
    {
        public enum Type
        {
            Min,
            Max,
            Mean,
            Avg,
            First,
            Last,
            Sum,
            Count,
        }

        public Type Aggregation;

        private double _val;
        private int _valIndex;
        public long Count;

        public TimeSeriesAggregation(int valIndex, TimeSeriesAggregation.Type type)
        {
            _valIndex = valIndex;
            Aggregation = type;
            _val = 0;
            Count = 0;
        }

        public void Init()
        {
            Count = 0;
            _val = 0;
        }

        public void Segment(Span<StatefulTimeStampValue> values)
        {
            var val = values[_valIndex];
            switch (Aggregation)
            {
                case Type.Min:
                    if (Count == 0)
                        _val = val.Min;
                    else
                        _val = Math.Min(_val, val.Min);
                    break;
                case Type.Max:
                    if (Count == 0)
                        _val = val.Max;
                    else
                        _val = Math.Max(_val, val.Max);
                    break;
                case Type.Sum:
                case Type.Avg:
                case Type.Mean:
                    _val += val.Sum;
                    break;
                case Type.First:
                    if (Count == 0)
                        _val = val.First;
                    break;
                case Type.Last:
                    _val = val.Last;
                    break;
                case Type.Count:
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
            }

            Count += val.Count;

        }

        public void Step(Span<double> values)
        {
            var val = values[_valIndex];
            switch (Aggregation)
            {
                case Type.Min:
                    if (Count == 0)
                        _val = val;
                    else
                        _val = Math.Min(_val, val);
                    break;
                case Type.Max:
                    if (Count == 0)
                        _val = val;
                    else
                        _val = Math.Max(_val, val);
                    break;
                case Type.Sum:
                case Type.Avg:
                case Type.Mean:
                    _val += val;
                    break;
                case Type.First:
                    if (Count == 0)
                        _val = val;
                    break;
                case Type.Last:
                    _val = val;
                    break;
                case Type.Count:
                    break;
                default:
                    throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
            }

            Count++;
        }

        public double GetFinalValue()
        {
            switch (Aggregation)
            {
                case Type.Min:
                case Type.Max:
                case Type.First:
                case Type.Last:
                    return _val;
                case Type.Count:
                    return Count;
                case Type.Sum:
                    return _val;
                case Type.Mean:
                case Type.Avg:
                    if (Count == 0)
                        return double.NaN;
                    return _val / Count;
                default:
                    throw new ArgumentOutOfRangeException("Unknown aggregation operation: " + Aggregation);
            }
        }
    }

}
