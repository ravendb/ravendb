using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using Acornima;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public sealed class QueryParser
    {
        private static readonly string[] OperatorStartMatches = { ">=", "<=", "<>", "<", ">", "==", "=", "!=", "BETWEEN", "IN", "ALL IN", "(" };
        private static readonly string[] BinaryOperators = { "OR", "AND" };
        private static readonly string[] StaticValues = { "true", "false", "null" };
        private static readonly string[] OrderByOptions = { "ASC", "DESC", "ASCENDING", "DESCENDING" };
        private static readonly string[] OrderByAsOptions = { "string", "long", "double", "alphaNumeric" };

        private int _depth;
        private QueryParser _timeSeriesParser;
        private NextTokenOptions _state = NextTokenOptions.Parenthesis;

        private int _statePos;

        private bool _insideTimeSeriesBody;
        private string _fromAlias;
        public const string TimeSeries = "timeseries";

        public QueryScanner Scanner;

        public void Init(string q)
        {
            _depth = 0;
            Scanner.Init(q);
        }

        public Query Parse(QueryType queryType = QueryType.Select, bool recursive = false)
        {
            if (!TryParse(out var query, out var message, queryType, recursive))
                ThrowParseException(message);

            return query;
        }

        public bool TryParse(out Query query, out string message, QueryType queryType = QueryType.Select, bool recursive = false)
        {
            query = new Query
            {
                QueryText = Scanner.Input
            };
            message = string.Empty;

            while (Scanner.TryScan("DECLARE"))
            {
                var func = DeclaredFunction();

                if (query.TryAddFunction(func) == false)
                {
                    message = $"{func.Name} function was declared multiple times";
                    return false;
                }
            }

            if (!TryParseFromClause(out var fromClause, out message))
                return false;

            query.From = fromClause;

            if (Scanner.TryScanMultiWordsToken("GROUP", "BY"))
                query.GroupBy = GroupBy();

            if (Scanner.TryScan("WHERE") && Expression(out query.Where) == false)
            {
                message = "Unable to parse WHERE clause";
                return false;
            }

            if (Scanner.TryScanMultiWordsToken("ORDER", "BY"))
                query.OrderBy = OrderBy();

            if (Scanner.TryScan("LOAD"))
                query.Load = SelectClauseExpressions("LOAD", false);


            if (Scanner.TryScan("FILTER") && Expression(out query.Filter) == false)
            {
                message = "Unable to parse FILTER clause";
                return false;
            }

            switch (queryType)
            {
                case QueryType.Select:
                    if (Scanner.TryScan("SELECT"))
                        query.Select = SelectClause("SELECT", query);
                    if (Scanner.TryScan("INCLUDE"))
                        query.Include = IncludeClause();
                    break;
                case QueryType.Update:

                    if (Scanner.TryScan("UPDATE") == false)
                    {
                        message = "Update operations must end with UPDATE clause";
                        return false;
                    }

                    var functionStart = Scanner.Position;
                    if (Scanner.FunctionBody() == false)
                    {
                        message = "Update clause must have a single function body";
                        return false;
                    }

                    query.UpdateBody = Scanner.Input.Substring(functionStart, Scanner.Position - functionStart);
                    try
                    {
                        // validate the js code
                        ValidateScript("function test()" + query.UpdateBody);
                    }
                    catch (Exception e)
                    {
                        var msg = AddLineAndColumnNumberToErrorMessage(e, "Update clause contains invalid script");
                        ThrowInvalidQueryException(msg, e);
                    }
                    break;
                default:
                    ThrowUnknownQueryType(queryType);
                    break;
            }

            Paging(out query.Offset, out query.Limit, out query.FilterLimit);
            if (queryType == QueryType.Select &&
                Scanner.TryScan("INCLUDE"))
            {
                if (query.Include != null)
                {
                    message = "Query already has an include statement, but encountered a second one";
                    return false;
                }
                query.Include = IncludeClause();
            }

            if (recursive == false && Scanner.AtEndOfInput() == false)
            {
                message = "Expected end of query";
                return false;
            }

            return true;
        }

        private DeclaredFunction SelectTimeSeries()
        {
            var start = Scanner.Position;

            Scanner.TimeSeriesFunctionBody();

            var functionText = Scanner.Input.Substring(start, Scanner.Position - start);

            if (_timeSeriesParser == null)
                _timeSeriesParser = new QueryParser();

            _timeSeriesParser.Init(functionText);
            _timeSeriesParser._fromAlias = _fromAlias;

            if (_timeSeriesParser.Scanner.TryScan('(') == false)
                ThrowParseException("Failed to find open parentheses ( for time series select function");

            var ts = _timeSeriesParser.ParseTimeSeriesBody("time series select function");

            if (_timeSeriesParser.Scanner.TryScan(')') == false)
                ThrowParseException("Failed to find closing parentheses ) for time series select function");

            return new DeclaredFunction
            {
                Type = AST.DeclaredFunction.FunctionType.TimeSeries,
                FunctionText = functionText,
                TimeSeries = ts
            };
        }


        private void Paging(out ValueExpression offset, out ValueExpression limit, out ValueExpression filterLimit)
        {
            offset = null;
            limit = null;
            filterLimit = null;

            var hasLimit = TryScanLimit(ref offset, ref limit);
            var hasOffset = TryScanOffset(ref offset);

            if (Scanner.TryScan("FILTER_LIMIT"))
            {
                if (Value(out filterLimit) == false)
                    ThrowInvalidQueryException("FILTER_LIMIT must contain a value");

                if (hasLimit == false)
                    TryScanLimit(ref offset, ref limit);

                if (hasOffset == false)
                    TryScanOffset(ref offset);
            }

            bool TryScanLimit(ref ValueExpression offset, ref ValueExpression limit)
            {
                if (Scanner.TryScan("LIMIT"))
                {
                    if (Value(out var first) == false)
                        ThrowInvalidQueryException("Limit must contain a value");

                    if (Scanner.TryScan(","))
                    {
                        if (Value(out var second) == false)
                            ThrowInvalidQueryException("Limit must contain a second value");

                        offset = first;
                        limit = second;
                    }
                    else
                    {
                        limit = first;
                    }

                    return true;
                }

                return false;
            }

            bool TryScanOffset(ref ValueExpression offset)
            {
                if (Scanner.TryScan("OFFSET"))
                {
                    if (offset != null)
                        ThrowInvalidQueryException("Cannot use 'offset' after 'limit $skip,$take'");

                    if (Value(out var second) == false)
                        ThrowInvalidQueryException("Offset must contain a value");

                    offset = second;

                    return true;
                }

                return false;
            }
        }

        [DoesNotReturn]
        private void ThrowInvalidQueryException(string message, Exception e = null)
        {
            throw new InvalidQueryException(message, Scanner.Input, null, e);
        }

        private static Acornima.Ast.Program ValidateScript(string script)
        {
            var javaScriptParser = new Acornima.Parser();
            return javaScriptParser.ParseScript(script);
        }

        [DoesNotReturn]
        private static void ThrowUnknownQueryType(QueryType queryType)
        {
            throw new ArgumentOutOfRangeException(nameof(queryType), queryType, "Unknown query type");
        }

        internal static string AddLineAndColumnNumberToErrorMessage(Exception e, string msg)
        {
            if (!(e is ParseErrorException pe))
                return msg;

            return $"{msg}{Environment.NewLine}At Line : {pe.LineNumber}, Column : {pe.Column}";
        }

        private List<QueryExpression> IncludeClause()
        {
            List<QueryExpression> includes = new List<QueryExpression>();

            do
            {
                if (Value(out var val))
                {
                    includes.Add(val);
                }
                else if (Field(out var field))
                {
                    QueryExpression expr;
                    if (Scanner.TryScan('('))
                    {
                        if (Method(field, out var method) == false)
                            ThrowParseException("Expected method call in " + field);
                        expr = method;
                    }
                    else
                    {
                        expr = field;
                    }

                    includes.Add(expr);
                }
                else
                {
                    ThrowParseException("Unable to understand include clause expression");
                }
            } while (Scanner.TryScan(","));
            return includes;
        }

        private DeclaredFunction DeclaredFunction()
        {
            // because of how we are processing them, we don't actually care for
            // parsing the function directly. We have implemented a minimal parser
            // here that find the _boundary_ of the function call, and then we hand
            // all of that code directly to the js code. 

            var functionStart = Scanner.Position;

            bool isFunc = Scanner.TryScan("function");

            if (isFunc == false)
            {
                if (Scanner.TryScan(TimeSeries) == false)
                    ThrowParseException("DECLARE clause found but missing 'function' keyword");
            }

            if (Scanner.Identifier() == false)
                ThrowParseException("DECLARE functions require a name and cannot be anonymous");

            var name = Scanner.Token;

            // this reads the signature of the method: (a,b,c), etc.
            // we are technically allowing more complex stuff there that isn't
            // allowed by JS, but that is fine, since the JS parser will break 
            // when it try it, so we are good with false positives here

            if (Scanner.TryScan('(') == false)
                ThrowParseException("Unable to parse  " + name + " signature");

            var parameters = ReadMethodArguments();

            var funcBodyStart = Scanner.Position;

            if (Scanner.FunctionBody() == false)
                ThrowParseException("Unable to get body for " + name);

            if (isFunc)
            {
                var functionText = Scanner.Input.Substring(functionStart, Scanner.Position - functionStart);

                // validate this function
                try
                {
                    var program = ValidateScript(functionText);
                    return new DeclaredFunction
                    {
                        FunctionText = functionText,
                        Name = name.Value,
                        JavaScript = program,
                        Type = AST.DeclaredFunction.FunctionType.JavaScript
                    };
                }
                catch (Exception e)
                {
                    var msg = AddLineAndColumnNumberToErrorMessage(e, $"Invalid script inside function {name}");
                    ThrowInvalidQueryException(msg, e);
                    return null;
                }
            }

            var functionBody = Scanner.Input.Substring(funcBodyStart, Scanner.Position - funcBodyStart);

            _timeSeriesParser ??= new QueryParser();
            _timeSeriesParser.Init(functionBody);

            if (_timeSeriesParser.Scanner.TryScan('{') == false)
                ThrowParseException($"Failed to find opening parentheses {{ for {name.Value}");

            var timeSeriesFunction = _timeSeriesParser.ParseTimeSeriesBody(name.Value);

            if (_timeSeriesParser.Scanner.TryScan('}') == false)
                ThrowParseException($"Failed to find opening parentheses }} for {name.Value}");

            return new DeclaredFunction
            {
                Type = AST.DeclaredFunction.FunctionType.TimeSeries,
                Name = name.Value,
                FunctionText = functionBody,
                TimeSeries = timeSeriesFunction,
                Parameters = parameters
            };
        }

        private TimeSeriesFunction ParseTimeSeriesBody(string name)
        {
            _insideTimeSeriesBody = true;

            var tsf = new TimeSeriesFunction();

            if (Scanner.TryScan("from") == false)
                ThrowParseException($"Unable to parse time series query for {name}, missing FROM");

            QueryExpression source;
            if (Field(out var field) == false)
            {
                if (Value(out var valueExpression) == false)
                    ThrowParseException($"Unable to parse time series query for {name}, missing FROM");

                source = valueExpression;
            }
            else
            {
                if (field.Compound.Count > 1 && field.Compound[0].Value == _fromAlias) // turn u.Heartrate into just Heartrate
                {
                    field.Compound.RemoveAt(0);
                }

                source = field;
            }

            tsf.Source = source;

            if (Scanner.TryScan("BETWEEN"))
            {
                tsf.Between = ReadTimeSeriesBetweenExpression(source);
            }

            if (Scanner.TryScan("First"))
            {
                if (tsf.Between != null)
                    ThrowCannotHaveBothClauses("First", "Between", name);

                var timeFromFirst = GetTimePeriodValueExpression(name, "First");

                if (Scanner.TryScan("BETWEEN"))
                    ThrowCannotHaveBothClauses("First", "Between", name);

                tsf.First = timeFromFirst;
            }

            if (Scanner.TryScan("LAST"))
            {
                if (tsf.Between != null)
                    ThrowCannotHaveBothClauses("Last", "Between", name);

                if (tsf.First != null)
                    ThrowCannotHaveBothClauses("First", "Last", name);

                var timeFromLast = GetTimePeriodValueExpression(name, "Last");

                if (Scanner.TryScan("BETWEEN"))
                    ThrowCannotHaveBothClauses("Last", "Between", name);

                tsf.Last = timeFromLast;
            }

            if (Scanner.TryScan("LOAD"))
            {
                var loadExpressions = SelectClauseExpressions("LOAD", false);

                if (loadExpressions.Count != 1)
                {
                    ThrowInvalidQueryException("Cannot have multiple LOAD fields in time series functions");
                }

                if (!(loadExpressions[0].Item1 is FieldExpression fe))
                {
                    ThrowInvalidQueryException($"Expected to have a field expression after LOAD, but got expression '{loadExpressions[0]}' of type '{loadExpressions[0].Item1.Type}'");
                    return null; //never hit

                }

                if (string.Equals("tag", fe.FieldValue, StringComparison.OrdinalIgnoreCase) == false)
                {
                    ThrowInvalidQueryException($"Expected to find 'Tag' after LOAD in time series function '{name}', but got '{fe.FieldValue}'." +
                                               "In time series functions you can only use LOAD in order to load by the 'Tag' property of a time series entry. " +
                                               "You can use the root RQL in order to load any other documents, and can pass them as arguments to the time series function");
                }

                tsf.LoadTagAs = loadExpressions[0].Item2;
            }

            if (Scanner.TryScan("WHERE"))
            {
                if (Expression(out var filter) == false)
                    ThrowInvalidQueryException($"Failed to parse filter expression after WHERE in time series function '{name}'");

                tsf.Where = filter;
            }

            if (Scanner.TryScanMultiWordsToken("GROUP", "BY"))
            {
                tsf.GroupBy.TimePeriod = GetTimePeriodValueExpression(name, "GROUP BY");

                if (Scanner.TryScan(","))
                {
                    if (Scanner.TryScan("TAG"))
                    {
                        tsf.GroupBy.Tag = true;
                    }
                    else if (Field(out var groupByField))
                    {
                        if (tsf.LoadTagAs == null)
                            ThrowInvalidQueryException("Group by field expression, is expected to be used with 'Load' document by tag.");

                        tsf.GroupBy.Field = groupByField;
                    }
                    else
                    {
                        ThrowParseException($"Expected 'Tag' or Field as a second group parameter in time series function '{name}'");
                    }
                }

                if (Scanner.TryScan("WITH"))
                {
                    if (Field(out var withField) && Scanner.TryScan('('))
                    {
                        if (Method(withField, out MethodExpression withExpr) == false)
                            ThrowParseException($"Expected method call after WITH in time series function '{name}'");
                        tsf.GroupBy.With = withExpr;
                    }
                    else
                    {
                        ThrowParseException($"Unable to parse WITH clause in time series function '{name}'");
                    }
                }
            }

            if (Scanner.TryScan("SELECT"))
            {
                tsf.Select = SelectClauseExpressions("SELECT", false);
            }

            if (Scanner.TryScan("SCALE"))
            {
                if (Value(out var scale) == false)
                    ThrowInvalidQueryException($"Failed to parse a value expression after SCALE in time series function '{name}'");

                tsf.Scale = scale;
            }

            if (Scanner.TryScan("OFFSET"))
            {
                if (Value(out var offset) == false)
                    ThrowInvalidQueryException($"Failed to parse a value expression after OFFSET in time series function '{name}'");

                tsf.Offset = offset;
            }

            _insideTimeSeriesBody = false;

            return tsf;
        }

        [DoesNotReturn]
        private void ThrowCannotHaveBothClauses(string clause1, string clause2, string functionName)
        {
            ThrowInvalidQueryException($"Cannot have both '{clause1}' and '{clause2}' in the same Time Series query function '{functionName}'");
        }

        private ValueExpression GetTimePeriodValueExpression(string functionName, string clause)
        {
            if (Value(out var timePeriod) == false)
            {
                string additionalInfo = null;
                if (Scanner.Identifier())
                {
                    additionalInfo = $@" Expected to get time period value but got '{Scanner.Token.Value}'. 
Grouping by 'Tag' or Field is supported only as a second grouping-argument.";

                }

                ThrowParseException($"Could not parse '{clause}' argument for '{functionName}. {additionalInfo}'");
            }

            if (timePeriod.Value == ValueTokenType.Long)
            {
                // we need to check 1h, 1d
                if (Scanner.Identifier())
                {
                    timePeriod.Token = new StringSegment(
                        timePeriod.Token.Buffer,
                        timePeriod.Token.Offset,
                        Scanner.Token.Offset + Scanner.Token.Length - timePeriod.Token.Offset);
                    timePeriod.Value = ValueTokenType.String;
                }
            }

            return timePeriod;
        }

        private List<(QueryExpression Expression, StringSegment? Alias)> GroupBy()
        {
            var fields = new List<(QueryExpression Expression, StringSegment? Alias)>();
            do
            {
                QueryExpression op = null;
                if (Field(out var field))
                {
                    if (Scanner.TryScan('('))
                    {
                        if (Method(field, out var method) == false)
                            ThrowParseException($"Unable to parse method call {field} for GROUP BY");
                        op = method;
                    }
                    else
                    {
                        op = field;
                    }
                }
                else if (Value(out var value))
                {
                    op = value;
                }
                else
                {
                    ThrowParseException("Unable to get field for GROUP BY");
                }

                Alias(false, out var alias);

                fields.Add((op, alias));

                if (Scanner.TryScan(",") == false)
                    break;

            } while (true);

            return fields;
        }

        private List<(QueryExpression Expression, OrderByFieldType OrderingType, bool Ascending)> OrderBy()
        {
            var orderBy = new List<(QueryExpression Expression, OrderByFieldType OrderingType, bool Ascending)>();
            do
            {
                if (Field(out var field) == false)
                    ThrowParseException("Unable to get field for ORDER BY");

                var type = OrderByFieldType.Implicit;

                QueryExpression op;
                if (Scanner.TryScan('('))
                {
                    if (Method(field, out var method) == false)
                        ThrowParseException($"Unable to parse method call {field} for ORDER BY");
                    op = method;
                }
                else
                {
                    op = field;
                }

                if (Scanner.TryScan("AS") && Scanner.TryScan(OrderByAsOptions, out var asMatch))
                {
                    switch (asMatch)
                    {
                        case "string":
                            type = OrderByFieldType.String;
                            break;
                        case "long":
                            type = OrderByFieldType.Long;
                            break;
                        case "double":
                            type = OrderByFieldType.Double;
                            break;
                        case "alphaNumeric":
                            type = OrderByFieldType.AlphaNumeric;
                            break;
                    }
                }

                var asc = true;

                if (Scanner.TryScan(OrderByOptions, out var match))
                {
                    if (match == "DESC" || match == "DESCENDING")
                        asc = false;
                }

                orderBy.Add((op, type, asc));

                if (Scanner.TryScan(",") == false)
                    break;
            } while (true);
            return orderBy;
        }

        private List<(QueryExpression, StringSegment?)> SelectClause(string clause, Query query)
        {
            query.IsDistinct = Scanner.TryScan("DISTINCT");

            if (Scanner.TryScan("*"))
                return null;

            var functionStart = Scanner.Position;

            if (Scanner.FunctionBody())
            {
                query.SelectFunctionBody.FunctionText = Scanner.Input.Substring(functionStart, Scanner.Position - functionStart);
                return new List<(QueryExpression, StringSegment?)>();
            }

            return SelectClauseExpressions(clause, aliasAsRequired: true, query: query);
        }

        private List<(QueryExpression, StringSegment?)> SelectClauseExpressions(string clause, bool aliasAsRequired, Query query = null)
        {
            var select = new List<(QueryExpression Expr, StringSegment? Alias)>();

            do
            {
                QueryExpression expr;
                if (Field(out var field))
                {
                    if (field.FieldValue == TimeSeries)
                    {
                        expr = GetTimeSeriesExpression(query);
                    }
                    else if (Scanner.TryScan('('))
                    {
                        if (Method(field, out var method) == false)
                            ThrowParseException("Expected method call in " + clause);
                        expr = method;
                    }
                    else
                    {
                        expr = field;
                    }
                }
                else if (Value(out var v))
                {
                    expr = v;
                }
                else
                {
                    ThrowParseException("Unable to get field for " + clause);
                    return null; // never called
                }

                if (Alias(aliasAsRequired, out var alias) == false && expr is ValueExpression ve)
                {
                    alias = ve.Token;
                }

                select.Add((expr, alias));

                if (Scanner.TryScan(",") == false)
                    break;
            } while (true);
            return select;
        }

        private MethodExpression GetTimeSeriesExpression(Query query)
        {
            _fromAlias = query?.From.Alias?.Value;

            var func = SelectTimeSeries();

            if (query?.TryAddTimeSeriesFunction(func) == false)
                ThrowParseException($"time series function '{func.Name}' was declared multiple times");

            var args = new List<QueryExpression>();

            if (func.TimeSeries.Source is FieldExpression f)
            {
                var compound = f.Compound;

                if (compound.Count > 1)
                {
                    if (_fromAlias != null)
                    {
                        args.Add(new FieldExpression(new List<StringSegment> { _fromAlias }));

                        if (_fromAlias != compound[0])
                        {
                            args.Add(new FieldExpression(new List<StringSegment> { compound[0] }));
                        }
                    }
                    else
                    {
                        args.Add(new FieldExpression(new List<StringSegment> { compound[0] }));
                    }

                    func.Parameters = args;
                }
            }

            return new MethodExpression(func.Name, args);
        }

        private bool TryParseFromClause(out FromClause fromClause, out string message)
        {
            fromClause = default;
            if (Scanner.TryScan("FROM") == false)
            {
                message = "Expected FROM clause";
                return false;
            }

            return TryParseExpressionAfterFromKeyword(out fromClause, out message);
        }

        private bool TryParseExpressionAfterFromKeyword(out FromClause fromClause, out string message)
        {
            FieldExpression field;
            QueryExpression filter = null;
            fromClause = default;
            message = string.Empty;
            var index = false;
            if (Scanner.TryScan("INDEX"))
            {
                if (Field(out field) == false)
                {
                    message = "Invalid syntax. Expected 'INDEX source' expression.";
                    return false;
                }

                index = true;
            }
            else
            {
                if (Field(out field) == false)
                {
                    message = "Unable to parse identifier in FROM clause";
                    return false;
                }

                if (Scanner.TryScan('(')) // Collection ( filter )
                {
                    if (Expression(out filter) == false)
                    {
                        message = "Expected filter in filtered FROM clause";
                        return false;
                    }

                    if (Scanner.TryScan(')') == false)
                    {
                        message = "Expected closing parenthesis in filtered FORM clause after filter";
                        return false;
                    }
                }
            }


            Alias(false, out var alias);

            fromClause = new FromClause
            {
                From = field,
                Alias = alias,
                Filter = filter,
                Index = index
            };


            return true;
        }

        private static readonly string[] AliasKeywords =
        {
            "AS",
            "SELECT",
            "WHERE",
            "LOAD",
            "GROUP",
            "ORDER",
            "INCLUDE",
            "UPDATE",
            "OFFSET",
            "LIMIT",
            "SCALE"
        };

        private bool Alias(bool aliasAsRequired, out StringSegment? alias)
        {
            bool required = false;
            if (Scanner.TryScan(AliasKeywords, out var match))
            {
                required = true;
                if (match != "AS")
                {
                    // found a keyword
                    Scanner.GoBack(match.Length);
                    alias = null;
                    return false;
                }
            }
            if (aliasAsRequired && required == false)
            {
                alias = null;
                return false;
            }

            var oldPos = Scanner.Position;
            if (Field(out var token))
            {
                if (required == false && // need to check that this is a real alias 
                    token.FieldValue.Equals("FILTER", StringComparison.OrdinalIgnoreCase))
                {
                    // if the alias is 'filter' *and* the next term is *not* a keyword, we have a filter clause so not an alias 
                    if (Scanner.TryPeek(AliasKeywords) == false)
                    {
                        Scanner.Reset(oldPos);
                        alias = null;
                        return false;
                    }
                }

                alias = token.FieldValue;
                return true;
            }

            if (required)
                ThrowParseException("Expected field alias after AS in SELECT");

            alias = null;
            return false;
        }

        internal bool Parameter(out StringSegment p)
        {
            if (Scanner.TryScan('$') == false)
            {
                p = default;
                return false;
            }

            if (Scanner.Identifier(false, true, true) == false)
                ThrowParseException("Expected parameter name");

            p = Scanner.Token;
            return true;
        }

        internal bool Expression(out QueryExpression op)
        {
            if (++_depth > 128)
                ThrowQueryException("Query is too complex, over 128 nested clauses are not allowed");
            if (Scanner.Position != _statePos)
            {
                _statePos = Scanner.Position;
                _state = NextTokenOptions.Parenthesis;
            }
            var result = Binary(out op);
            _depth--;
            return result;
        }

        private bool Binary(out QueryExpression op)
        {
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
            {
                ThrowQueryTooComplexException();

                op = null; // code is unreachable
                return false;
            }

            switch (_state)
            {
                case NextTokenOptions.Parenthesis:
                    if (Parenthesis(out op) == false)
                        return false;
                    break;
                case NextTokenOptions.BinaryOp:
                    _state = NextTokenOptions.Parenthesis;
                    if (Operator(OperatorField.Required, out op) == false)
                        return false;
                    break;
                default:
                    op = null;
                    return false;
            }


            if (Scanner.TryScan(BinaryOperators, out var found) == false)
                return true; // found simple

            var negate = Scanner.TryScan("NOT");
            var type = found == "OR"
                ? OperatorType.Or
                : OperatorType.And;

            _state = NextTokenOptions.Parenthesis;

            if (Binary(out var right) == false)
                ThrowParseException($"Failed to find second part of {type} expression");

            bool allowSimplificationOfOperation = !(right is BinaryExpression be) || be.Parenthesis == false;
            return TrySimplifyBinaryExpression(right, type, negate, allowSimplificationOfOperation, ref op);
        }

        private bool TrySimplifyBinaryExpression(QueryExpression right,
            OperatorType type,
            bool negate,
            bool allowSimplification,
            ref QueryExpression op)
        {
            if (allowSimplification)
            {
                if (negate)
                {
                    right = NegateExpressionWithoutParenthesis(right);
                }

                // if the other arg isn't parenthesis, use operator precedence rules
                // to re-write the query
                switch (type)
                {
                    case OperatorType.And:
                        if (right is BinaryExpression rightOp)
                        {
                            switch (rightOp.Operator)
                            {
                                case OperatorType.Or:
                                case OperatorType.And:

                                    rightOp.Left = new BinaryExpression(op, rightOp.Left, type);
                                    op = right;
                                    return true;
                            }
                        }
                        break;
                }
            }
            else if (negate)
            {
                right = new NegatedExpression(right);
            }

            op = new BinaryExpression(op, right, type);

            return true;
        }

        private QueryExpression NegateExpressionWithoutParenthesis(QueryExpression expr)
        {
            bool ShouldRecurse(BinaryExpression e)
            {
                if (e.Parenthesis)
                    return false;

                return e.Operator == OperatorType.And ||
                       e.Operator == OperatorType.Or;
            }

            if (expr is BinaryExpression be && ShouldRecurse(be))
            {
                var result = be;

                while (be.Left is BinaryExpression nested && ShouldRecurse(nested))
                {
                    be = nested;
                }
                be.Left = new NegatedExpression(be.Left);

                return result;
            }
            return new NegatedExpression(expr);
        }

        private bool Parenthesis(out QueryExpression op)
        {
            if (Scanner.TryScan('(') == false)
            {
                _state = NextTokenOptions.BinaryOp;
                return Binary(out op);
            }

            if (Expression(out op) == false)
                return false;

            if (Scanner.TryScan(')') == false)
                ThrowParseException("Unmatched parenthesis, expected ')'");

            if (op is BinaryExpression be)
            {
                be.Parenthesis = true;
            }

            return true;
        }

        private enum OperatorField
        {
            Required,
            Optional,
            Desired
        }

        private bool Operator(OperatorField fieldOption, out QueryExpression op)
        {
            OperatorType type;
            FieldExpression field = null;

            if (Scanner.TryScan("true"))
            {
                op = new TrueExpression();
                return true;
            }

            if (fieldOption != OperatorField.Optional && Field(out field) == false)
            {
                op = null;
                return false;
            }

            if (Scanner.TryScan(OperatorStartMatches, out var match) == false)
            {
                if (fieldOption != OperatorField.Required)
                {
                    op = field;
                    return fieldOption == OperatorField.Desired;
                }
                ThrowInvalidQueryException($"Expected operator after '{field?.FieldValue ?? "<failed to fetch field name>"}' field, but found '{Scanner.Input[Scanner.Position]}'. Valid operators are: 'in', 'between', =, <, >, <=, >=, !=");
            }


            switch (match)
            {
                case "<":
                    type = OperatorType.LessThan;
                    break;
                case ">":
                    type = OperatorType.GreaterThan;
                    break;
                case "<=":
                    type = OperatorType.LessThanEqual;
                    break;
                case ">=":
                    type = OperatorType.GreaterThanEqual;
                    break;
                case "=":
                case "==":
                    type = OperatorType.Equal;
                    break;
                case "!=":
                case "<>":
                    type = OperatorType.NotEqual;
                    break;
                case "BETWEEN":
                    op = _insideTimeSeriesBody
                        ? ReadTimeSeriesBetweenExpression(field)
                        : ReadBetweenExpression(field);
                    return true;
                case "IN":
                case "ALL IN":
                    if (Scanner.TryScan('(') == false)
                        ThrowParseException("parsing In, expected '('");

                    var list = new List<QueryExpression>();
                    do
                    {
                        if (Scanner.TryScan(')'))
                            break;

                        if (list.Count != 0)
                            if (Scanner.TryScan(',') == false)
                                ThrowParseException("parsing In expression, expected ','");

                        if (Value(out var inVal) == false)
                            ThrowParseException("parsing In, expected a value");

                        if (list.Count > 0)
                            if (list[0].Type != inVal.Type)
                                ThrowQueryException(
                                    $"Invalid In expression, all values must have the same type, expected {list[0].Type} but got {inVal.Type}");
                        list.Add(inVal);
                    } while (true);

                    op = new InExpression(field, list, match == "ALL IN");

                    return true;
                case "(":
                    var isMethod = Method(field, out var method);
                    op = method;

                    if (isMethod && Operator(OperatorField.Optional, out var methodOperator))
                    {
                        if (methodOperator is BinaryExpression be)
                        {
                            be.Left = method;
                            op = be;
                            return true;
                        }

                        if (methodOperator is InExpression ie)
                        {
                            ie.Source = method;
                            op = ie;
                            return true;
                        }
                        if (methodOperator is BetweenExpression between)
                        {
                            between.Source = method;
                            op = between;
                            return true;
                        }
                        if (methodOperator is MethodExpression me)
                        {
                            op = me;
                            return true;
                        }
                        ThrowParseException("Unexpected operator after method call: " + methodOperator);
                    }

                    return isMethod;
                default:
                    op = null;
                    return false;
            }

            if (Value(out var val))
            {
                op = new BinaryExpression(field, val, type);
                return true;
            }

            if (Operator(OperatorField.Desired, out var op2))
            {
                op = new BinaryExpression(field, op2, type);
                return true;
            }

            op = null;
            return false;
        }

        private BetweenExpression ReadBetweenExpression(FieldExpression field)
        {
            if (Value(out var fst) == false)
                ThrowParseException("parsing Between, expected value (1st)");
            if (Scanner.TryScan("AND") == false)
                ThrowParseException("parsing Between, expected AND");
            if (Value(out var snd) == false)
                ThrowParseException("parsing Between, expected value (2nd)");

            if (fst.Type != snd.Type)
                ThrowQueryException(
                    $"Invalid Between expression, values must have the same type but got {fst.Type} and {snd.Type}");

            return new BetweenExpression(field, fst, snd);
        }


        private TimeSeriesBetweenExpression ReadTimeSeriesBetweenExpression(QueryExpression source)
        {
            QueryExpression minExpression, maxExpression;
            if (Value(out var val) == false)
            {
                if (Field(out var f) == false)
                    ThrowParseException("parsing Between, expected value or field (1st)");

                minExpression = f;
            }
            else
            {
                minExpression = val;
            }

            if (Scanner.TryScan("AND") == false)
                ThrowParseException("parsing Between, expected AND");

            if (Value(out val) == false)
            {
                if (Field(out var f) == false)
                    ThrowParseException("parsing Between, expected value or field (2nd)");

                maxExpression = f;
            }
            else
            {
                maxExpression = val;
            }

            return new TimeSeriesBetweenExpression(source, minExpression, maxExpression);
        }

        private bool Method(FieldExpression field, out MethodExpression op)
        {
            var args = ReadMethodArguments();

            op = new MethodExpression(field.FieldValue, args);
            return true;
        }

        private List<QueryExpression> ReadMethodArguments()
        {
            var args = new List<QueryExpression>();
            do
            {
                if (Scanner.TryScan(')'))
                    break;

                if (args.Count != 0)
                {
                    if (Scanner.TryScan("as"))
                    {
                        var readAlias = Scanner.Identifier();

                        var lastArg = args[args.Count - 1];
                        if (lastArg.Type != ExpressionType.Method)
                        {
                            if (readAlias)
                                ThrowInvalidQueryException($"Alias '{Scanner.Token}' can only be applied on method, but was applied on '{lastArg.Type}'");

                            ThrowInvalidQueryException($"Alias can only be applied on method, but was applied on '{lastArg.Type}'");
                        }

                        var method = (MethodExpression)lastArg;

                        if (readAlias == false)
                            ThrowInvalidQueryException($"Missing alias for method '{method.Name}' after 'as'");

                        method.Alias = Scanner.Token;

                        if (Scanner.TryScan(')'))
                            break;
                    }

                    if (Scanner.TryScan(',') == false)
                        ThrowParseException("parsing method expression, expected ','");
                }

                var maybeExpression = false;
                var position = Scanner.Position;
                if (Value(out var argVal))
                {
                    if (Scanner.TryPeek(',') == false && Scanner.TryPeek(')') == false)
                    {
                        // this is not a simple field ref, let's parse as full expression

                        Scanner.Reset(position); // if this was a value then it had to be in ''
                        maybeExpression = true;
                    }
                    else
                    {
                        args.Add(argVal);
                        continue;
                    }
                }

                if (maybeExpression == false && Field(out var fieldRef))
                {
                    if (Scanner.TryPeek(',') == false && Scanner.TryPeek(')') == false)
                    {
                        // this is not a simple field ref, let's parse as full expression

                        Scanner.Reset(fieldRef.Compound[0].Offset);
                    }
                    else
                    {
                        args.Add(fieldRef);
                        continue;
                    }
                }

                if (Expression(out var expr))
                    args.Add(expr);
                else
                    ThrowParseException("parsing method, expected an argument");
            } while (true);
            return args;
        }

        [DoesNotReturn]
        private void ThrowParseException(string msg)
        {
            var sb = new StringBuilder()
                .Append(Scanner.Line)
                .Append(":")
                .Append(Scanner.Column)
                .Append(" ")
                .Append(msg)
                .Append(" but got");

            if (Scanner.NextToken())
                sb.Append(": ")
                    .Append(Scanner.CurrentToken);
            else
                sb.Append(" to the end of the query");


            sb.AppendLine();
            sb.AppendLine("Query: ");
            sb.Append(Scanner.Input);

            throw new ParseException(sb.ToString());
        }

        [DoesNotReturn]
        private void ThrowQueryException(string msg)
        {
            var sb = new StringBuilder()
                .Append(Scanner.Column)
                .Append(":")
                .Append(Scanner.Line)
                .Append(" ")
                .Append(msg);

            throw new ParseException(sb.ToString());
        }

        [DoesNotReturn]
        private void ThrowQueryTooComplexException()
        {
            throw new ParseException($"Query is too complex. Query: {Scanner.Input}");
        }

        private bool Value(out ValueExpression val)
        {
            var numberToken = Scanner.TryNumber();
            if (numberToken != null)
            {
                val = new ValueExpression(Scanner.Token,
                    numberToken.Value == NumberToken.Long ? ValueTokenType.Long : ValueTokenType.Double
                );
                return true;
            }
            if (Scanner.String(out var token))
            {
                val = new ValueExpression(
                    token,
                    ValueTokenType.String
                );
                return true;
            }
            if (Scanner.TryScan(StaticValues, out var match))
            {
                ValueTokenType type;
                switch (match)
                {
                    case "true":
                        type = ValueTokenType.True;
                        break;
                    case "false":
                        type = ValueTokenType.False;
                        break;
                    case "null":
                        type = ValueTokenType.Null;
                        break;
                    default:
                        type = ValueTokenType.String;
                        break;
                }

                val = new ValueExpression(
                    Scanner.Token,
                    type);
                return true;
            }

            if (Parameter(out _))
            {
                val = new ValueExpression(
                    Scanner.Token,
                    ValueTokenType.Parameter
                );
                return true;
            }
            val = null;
            return false;
        }

        internal bool Field(out FieldExpression token)
        {
            var part = 0;

            var parts = new List<StringSegment>(1);
            bool quoted = false;
            while (true)
            {
                if (Scanner.Identifier(beginning: part++ == 0) == false)
                {
                    if (Scanner.String(out var str, fieldPath: part > 1))
                    {
                        if (part == 1 ||
                            part == 2 && _insideTimeSeriesBody && parts[0].Value == _fromAlias)
                            quoted = true;
                        parts.Add(str);
                    }
                    else
                    {
                        token = null;
                        return false;
                    }
                }
                else
                {
                    parts.Add(Scanner.Token);
                }
                if (part == 1)
                {
                    // need to ensure that this isn't a keyword
                    if (Scanner.CurrentTokenMatchesAnyOf(AliasKeywords))
                    {
                        Scanner.GoBack();
                        token = null;
                        return false;
                    }
                }

                bool? hasNextPart = null;
                while (Scanner.TryScan('['))
                {
                    switch (Scanner.TryNumber())
                    {
                        case NumberToken.Long:
                            if (Scanner.TryScan(']') == false)
                                ThrowParseException("Expected to find closing ]");
                            parts.Add(Scanner.Token);
                            break;

                        case null:
                            if (Scanner.TryPeek('.'))
                                ThrowInvalidQueryException("Expected to find closing ']'. If this is an array indexer expression, the correct syntax would be 'collection[].MemberFieldName'");
                            if (Scanner.TryScan(']') == false)
                                ThrowInvalidQueryException($"Expected to find closing ']' after '{Scanner.Token}['.");
                            parts.Add("[]");

                            break;
                        case NumberToken.Double:
                            ThrowParseException("Array indexer must be integer, but got double");
                            break;
                    }

                    hasNextPart = Scanner.TryScan('.');
                }

                if (hasNextPart == true)
                    continue;

                if (Scanner.TryScan('.') == false)
                    break;
            }


            token = new FieldExpression(parts)
            {
                IsQuoted = quoted
            };

            return true;
        }

        private enum NextTokenOptions
        {
            Parenthesis,
            BinaryOp
        }

        public sealed class ParseException : Exception
        {
            public ParseException(string msg) : base(msg)
            {
            }
        }
    }
}
