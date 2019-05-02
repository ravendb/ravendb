using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Exceptions;
using Sparrow;

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

                if(existing.withQuery.From.From.Compound.Count == 0)
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
        public QueryExpression Between;
        public ValueExpression GroupBy;
        public List<(QueryExpression, StringSegment?)> Select;

      

        public RangeGroup ParseFromString(string s)
        {
            var range = new RangeGroup();
            var offset = 0;

            while (offset < s.Length)
            {
                var duration = ParseNumber(s, ref offset);
                ParseRange(s, ref offset, range, duration);
            }

            return range;
        }

        private void ParseRange(string source, ref int offset, RangeGroup range, int duration)
        {
            while (char.IsWhiteSpace(source[offset]))
            {
                offset++;
            }

            switch (char.ToLower(source[offset++]))
            {
                case 's':
                    if(TryConsumeMatch(source, ref offset, "seconds") == false)
                        TryConsumeMatch(source, ref offset, "second");

                    range.Seconds += duration;
                    return;
                case 'm':
                    if (TryConsumeMatch(source, ref offset, "minutes") ||
                        TryConsumeMatch(source, ref offset, "min"))
                    {
                        range.Minutes += duration;
                        return;
                    }

                    if (TryConsumeMatch(source, ref offset, "ms") ||
                        TryConsumeMatch(source, ref offset, "milli") ||
                        TryConsumeMatch(source, ref offset, "milliseconds"))
                    {
                        range.Milliseconds += duration;
                        return;
                    }
                    if (TryConsumeMatch(source, ref offset, "months") ||
                        TryConsumeMatch(source, ref offset, "month") ||
                        TryConsumeMatch(source, ref offset, "mon"))
                    {
                        range.Months+= duration;
                        return;
                    }
                    range.Minutes += duration;
                    return;
                case 'd':
                    if (TryConsumeMatch(source, ref offset, "days") == false)
                        TryConsumeMatch(source, ref offset, "day");
                    range.Days += duration;
                    return;
                case 'w':
                    if (TryConsumeMatch(source, ref offset, "weeks") == false)
                        TryConsumeMatch(source, ref offset, "week");
                    range.Weeks += duration;
                    return;
                case 'q':
                    if (TryConsumeMatch(source, ref offset, "quarters") == false)
                        TryConsumeMatch(source, ref offset, "quarter");
                    range.Quarters += duration;
                    return;

                case 'y':
                    if (TryConsumeMatch(source, ref offset, "years") == false)
                        TryConsumeMatch(source, ref offset, "year");
                    range.Years += duration;
                    return;
                default:
                    throw new ArgumentException("Unable to understand time range: " + source.Substring(offset));
            }
        }

        private bool TryConsumeMatch(string source, ref int offset, string additionalMatch)
        {
            if(new StringSegment(source, offset-1 , source.Length - offset -1).StartsWith(additionalMatch, StringComparison.OrdinalIgnoreCase))
            {
                offset += additionalMatch.Length-1;
                return true;
            }
            return false;
        }

        private int ParseNumber(string source, ref int offset)
        {
            int i;
            for (i= offset; i < source.Length; i++)
            {
                if (char.IsWhiteSpace(source[i]))
                    continue;
            }

            for (; i < source.Length; i++)
            {
                if (char.IsNumber(source[i]) == false)
                    break;
            }

            fixed(char* s = source)
            {
                if (int.TryParse(new ReadOnlySpan<char>(s + i, source.Length - i), out var amount) )
                {
                    offset = i;
                    return amount;
                }
            }

            throw new ArgumentException("Unable to parse: '" + source.Substring(offset) + "' as a number");
        }
    }
}
