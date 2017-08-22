using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public class QueryParser
    {
        private static readonly string[] OperatorStartMatches = { ">=", "<=", "<", ">", "=", "==", "BETWEEN", "IN", "ALL IN", "(" };
        private static readonly string[] BinaryOperators = { "OR", "AND" };
        private static readonly string[] StaticValues = { "true", "false", "null" };
        private static readonly string[] OrderByOptions = { "ASC", "DESC", "ASCENDING", "DESCENDING" };
        private static readonly string[] OrderByAsOptions = { "string", "long", "double", "alphaNumeric" };


        private int _depth;
        private NextTokenOptions _state = NextTokenOptions.Parenthesis;

        private int _statePos;

        public QueryScanner Scanner = new QueryScanner();

        public void Init(string q)
        {
            _depth = 0;
            Scanner.Init(q);
        }

        public Query Parse()
        {
            var q = new Query
            {
                QueryText = Scanner.Input
            };

            while (Scanner.TryScan("DECLARE"))
            {
                var (name, func) = DeclaredFunction();
             
                if (q.TryAddFunction(name, QueryExpression.Extract(Scanner.Input, func)) == false)
                    ThrowParseException(name + " function was declared multiple times");
            }

            if (Scanner.TryScan("SELECT"))
                q.Select = SelectClause("SELECT", q);

            q.From = FromClause();

            if (Scanner.TryScan("WITH"))
                q.With = SelectClauseExpressions("WITH");

            if (Scanner.TryScan("GROUP BY"))
                q.GroupBy = GroupBy();

            if (Scanner.TryScan("WHERE") && Expression(out q.Where) == false)
                ThrowParseException("Unable to parse WHERE clause");

            if (Scanner.TryScan("ORDER BY"))
                q.OrderBy = OrderBy();

            if (Scanner.TryScan("SELECT"))
            {
                if(q.Select!= null)
                    ThrowParseException("Only a single SELECT clause is allowed, but got two");

                q.Select = SelectClause("SELECT", q);
            }

            if (Scanner.NextToken())
                ThrowParseException("Expected end of query");

            return q;
        }

        private (StringSegment Name, ValueToken FunctionText) DeclaredFunction()
        {
            // becuase of how we are processing them, we don't actually care for
            // parsing the function directly. We have implemented a minimal parser
            // here that find the _boundary_ of the function call, and then we hand
            // all of that code directly to the js code. 

            var functionStart = Scanner.Position;

            if (Scanner.TryScan("function") == false)
                ThrowParseException("DECLARE clause found but missing 'function' keyword");

            if (Scanner.Identifier() == false)
                ThrowParseException("DECLARE functions require a name and cannot be anonymous");

            var name = new StringSegment(Scanner.Input, Scanner.TokenStart, Scanner.TokenLength);

            // this reads the signature of the method: (a,b,c), etc.
            // we are technically allow more complex stuff there that isn't
            // allowed by JS, but that is fine, since the JS parser will break 
            // when it try it, so we are good with false positives here

            if (Scanner.TryScan('(') == false)
                ThrowParseException("Unable to parse function " + name + " signature");

            if (Method(null, out _) == false)
                ThrowParseException("Unable to parse function " + name + " signature");

            if(Scanner.FunctionBody() == false)
                ThrowParseException("Unable to get function body for " + name);

            return (name, new ValueToken
            {
                Type = ValueTokenType.String,
                TokenStart = functionStart,
                TokenLength = Scanner.Position - functionStart
            });
        }

        private List<FieldToken> GroupBy()
        {
            var fields = new List<FieldToken>();
            do
            {
                if (Field(out var field) == false)
                    ThrowParseException("Unable to get field for GROUP BY");

                fields.Add(field);

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

                OrderByFieldType type = OrderByFieldType.Implicit;

                QueryExpression op;
                if (Scanner.TryScan('('))
                {
                    if (Method(field, out op) == false)
                        ThrowParseException($"Unable to parse method call {QueryExpression.Extract(Scanner.Input, field)}for ORDER BY");
                }
                else
                {
                    op = new QueryExpression
                    {
                        Field = field,
                        Type = OperatorType.Field
                    };
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

        private List<(QueryExpression, FieldToken)> SelectClause(string clause, Query query)
        {
            query.IsDistinct = Scanner.TryScan("DISTINCT");

            if (Scanner.TryScan("*"))
                return null;

            var functionStart = Scanner.Position;
            if (Scanner.FunctionBody())
            {
                query.SelectFunctionBody = new ValueToken
                {
                    Type = ValueTokenType.String,
                    TokenStart = functionStart,
                    TokenLength = Scanner.Position - functionStart
                };

                return new List<(QueryExpression, FieldToken)>();
            }

            return SelectClauseExpressions(clause);
        }

        private List<(QueryExpression, FieldToken)> SelectClauseExpressions(string clause)
        {
            var select = new List<(QueryExpression Expr, FieldToken Id)>();

            do
            {
                QueryExpression expr;
                if (Field(out var field))
                {
                    if (Scanner.TryScan('('))
                    {
                        if (Method(field, op: out expr) == false)
                            ThrowParseException("Expected method call in " + clause);
                    }
                    else
                    {
                        expr = new QueryExpression
                        {
                            Field = field,
                            Type = OperatorType.Field
                        };
                    }
                }
                else if (Value(out var v))
                {
                    expr = new QueryExpression
                    {
                        Value = v,
                        Type = OperatorType.Value
                    };
                }
                else
                {
                    ThrowParseException("Unable to get field for " + clause);
                    return null; // never callsed
                }

                if (Alias(out var alias) == false && expr.Type == OperatorType.Value)
                {
                    alias = new FieldToken
                    {
                        EscapeChars = expr.Value.EscapeChars,
                        IsQuoted = expr.Value.Type == ValueTokenType.String,
                        TokenStart = expr.Value.TokenStart,
                        TokenLength = expr.Value.TokenLength
                    };
                }

                @select.Add((expr, alias));

                if (Scanner.TryScan(",") == false)
                    break;
            } while (true);
            return @select;
        }

        private (FieldToken From, FieldToken Alias, QueryExpression Filter, bool Index) FromClause()
        {
            if (Scanner.TryScan("FROM") == false)
                ThrowParseException("Expected FROM clause");

            FieldToken field;
            QueryExpression filter = null;
            bool index = false;
            bool isQuoted;
            if (Scanner.TryScan('(')) // FROM ( Collection, filter )
            {
                isQuoted = false;
                if (!Scanner.Identifier() && !(isQuoted = Scanner.String()))
                    ThrowParseException("Expected FROM source");

                field = new FieldToken
                {
                    TokenLength = Scanner.TokenLength,
                    TokenStart = Scanner.TokenStart,
                    EscapeChars = Scanner.EscapeChars,
                    IsQuoted = isQuoted
                };

                if (Scanner.TryScan(',') == false)
                    ThrowParseException("Expected COMMA in filtered FORM clause after source");

                if (Expression(out filter) == false)
                    ThrowParseException("Expected filter in filtered FORM clause");

                if (Scanner.TryScan(')') == false)
                    ThrowParseException("Expected closing parenthesis in filtered FORM clause after filter");
            }
            else if (Scanner.TryScan("INDEX"))
            {
                isQuoted = false;
                if (!Scanner.Identifier() && !(isQuoted = Scanner.String()))
                    ThrowParseException("Expected FROM INDEX source");

                field = new FieldToken
                {
                    TokenLength = Scanner.TokenLength,
                    TokenStart = Scanner.TokenStart,
                    EscapeChars = Scanner.EscapeChars,
                    IsQuoted = isQuoted
                };

                index = true;
            }
            else
            {
                isQuoted = false;
                if (!Scanner.Identifier() && !(isQuoted = Scanner.String()))
                    ThrowParseException("Expected FROM source");

                field = new FieldToken
                {
                    TokenLength = Scanner.TokenLength,
                    TokenStart = Scanner.TokenStart,
                    EscapeChars = Scanner.EscapeChars,
                    IsQuoted = isQuoted
                };
            }

            FieldToken alias = null;
            if (Scanner.TryScan("AS"))
            {
                isQuoted = false;
                if (!Scanner.Identifier() && !(isQuoted = Scanner.String()))
                    ThrowParseException("Expected ALIAS after AS in FROM");

                alias = new FieldToken
                {
                    TokenLength = Scanner.TokenLength,
                    TokenStart = Scanner.TokenStart,
                    EscapeChars = Scanner.EscapeChars,
                    IsQuoted = isQuoted
                };

            }

            return (field, alias, filter, index);
        }

        private bool Alias(out FieldToken alias)
        {
            if (Scanner.TryScan("AS") == false)
            {
                alias = null;
                return false;
            }

            if (Field(out alias) == false)
                ThrowParseException("Expected field alias after AS in SELECT");

            return true;
        }

        internal bool Parameter(out int tokenStart, out int tokenLength)
        {
            if (Scanner.TryScan(':') == false)
            {
                tokenStart = 0;
                tokenLength = 0;
                return false;
            }

            Scanner.TokenStart = Scanner.Position;

            tokenStart = Scanner.TokenStart;

            if (Scanner.Identifier(false) == false)
                ThrowParseException("Expected parameter name");

            tokenLength = Scanner.TokenLength;
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
            switch (_state)
            {
                case NextTokenOptions.Parenthesis:
                    if (Parenthesis(out op) == false)
                        return false;
                    break;
                case NextTokenOptions.BinaryOp:
                    _state = NextTokenOptions.Parenthesis;
                    if (Operator(true, out op) == false)
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
                ? (negate ? OperatorType.OrNot : OperatorType.Or)
                : (negate ? OperatorType.AndNot : OperatorType.And);

            _state = NextTokenOptions.Parenthesis;

            var parenthesis = Scanner.TryPeek('(');

            if (Binary(out var right) == false)
                ThrowParseException($"Failed to find second part of {type} expression");

            if (parenthesis == false)
            {
                // if the other arg isn't parenthesis, use operator precedence rules
                // to re-write the query
                switch (type)
                {
                    case OperatorType.And:
                    case OperatorType.AndNot:
                        switch (right.Type)
                        {
                            case OperatorType.AndNot:
                            case OperatorType.OrNot:
                            case OperatorType.Or:
                            case OperatorType.And:

                                right.Left = new QueryExpression
                                {
                                    Left = op,
                                    Right = right.Left,
                                    Type = type
                                };
                                op = right;
                                return true;
                        }

                        break;
                }
            }


            op = new QueryExpression
            {
                Type = type,
                Left = op,
                Right = right
            };

            return true;
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
            return true;
        }

        private bool Operator(bool fieldRequired, out QueryExpression op)
        {
            OperatorType type;
            FieldToken field = null;

            if (Scanner.TryScan("true"))
                type = OperatorType.True;
            else
            {
                if (fieldRequired && Field(out field) == false)
                {
                    op = null;
                    return false;
                }

                if (Scanner.TryScan(OperatorStartMatches, out var match) == false)
                {
                    if (fieldRequired == false)
                    {
                        op = null;
                        return false;
                    }
                    ThrowParseException("Invalid operator expected any of (In, Between, =, <, >, <=, >=)");
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
                    case "BETWEEN":
                        type = OperatorType.Between;
                        break;
                    case "IN":
                        type = OperatorType.In;
                        break;
                    case "ALL IN":
                        type = OperatorType.AllIn;
                        break;
                    case "(":
                        type = OperatorType.Method;
                        break;
                    default:
                        op = null;
                        return false;
                }
            }

            switch (type)
            {
                case OperatorType.True:
                    op = new QueryExpression
                    {
                        Type = type
                    };
                    return true;
                case OperatorType.Method:
                    var method = Method(field, op: out op);

                    if (method && Operator(false, out var methodOperator))
                    {
                        if (op.Arguments == null)
                            op.Arguments = new List<object>();

                        op.Arguments.Add(methodOperator);
                        return true;
                    }

                    return method;
                case OperatorType.Between:
                    if (Value(out var fst) == false)
                        ThrowParseException("parsing Between, expected value (1st)");
                    if (Scanner.TryScan("AND") == false)
                        ThrowParseException("parsing Between, expected AND");
                    if (Value(out var snd) == false)
                        ThrowParseException("parsing Between, expected value (2nd)");

                    if (fst.Type != snd.Type)
                        ThrowQueryException(
                            $"Invalid Between expression, values must have the same type but got {fst.Type} and {snd.Type}");

                    op = new QueryExpression
                    {
                        Field = field,
                        Type = OperatorType.Between,
                        First = fst,
                        Second = snd
                    };
                    return true;
                case OperatorType.In:
                case OperatorType.AllIn:
                    if (Scanner.TryScan('(') == false)
                        ThrowParseException("parsing In, expected '('");

                    var list = new List<ValueToken>();
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

                    op = new QueryExpression
                    {
                        Field = field,
                        Type = type,
                        Values = list
                    };

                    return true;
                default:
                    if (Value(out var val) == false)
                        ThrowParseException($"parsing {type} expression, expected a value");

                    op = new QueryExpression
                    {
                        Field = field,
                        Type = type,
                        Value = val
                    };
                    return true;
            }
        }

        private bool Method(FieldToken field, out QueryExpression op)
        {
            var args = new List<object>();
            do
            {
                if (Scanner.TryScan(')'))
                    break;

                if (args.Count != 0)
                    if (Scanner.TryScan(',') == false)
                        ThrowParseException("parsing method expression, expected ','");

                if (Value(out var argVal))
                {
                    args.Add(argVal);
                    continue;
                }

                if (Field(out var fieldRef))
                {
                    if (Scanner.TryPeek(',') == false && Scanner.TryPeek(')') == false)
                    {
                        // this is not a simple field ref, let's parse as full expression

                        Scanner.Reset(fieldRef.TokenStart);
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

            op = new QueryExpression
            {
                Field = field,
                Type = OperatorType.Method,
                Arguments = args
            };
            return true;
        }

        private void ThrowParseException(string msg)
        {
            var sb = new StringBuilder()
                .Append(Scanner.Column)
                .Append(":")
                .Append(Scanner.Line)
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

        private bool Value(out ValueToken val)
        {
            var numberToken = Scanner.TryNumber();
            if (numberToken != null)
            {
                val = new ValueToken
                {
                    TokenStart = Scanner.TokenStart,
                    TokenLength = Scanner.TokenLength,
                    Type = numberToken.Value == NumberToken.Long ? ValueTokenType.Long : ValueTokenType.Double
                };
                return true;
            }
            if (Scanner.String())
            {
                val = new ValueToken
                {
                    TokenStart = Scanner.TokenStart,
                    TokenLength = Scanner.TokenLength,
                    Type = ValueTokenType.String,
                    EscapeChars = Scanner.EscapeChars
                };
                return true;
            }
            if (Scanner.TryScan(StaticValues, out var match))
            {
                val = new ValueToken
                {
                    TokenStart = Scanner.TokenStart,
                    TokenLength = Scanner.TokenLength
                };
                switch (match)
                {
                    case "true":
                        val.Type = ValueTokenType.True;
                        break;
                    case "false":
                        val.Type = ValueTokenType.False;
                        break;
                    case "null":
                        val.Type = ValueTokenType.Null;
                        break;
                }

                return true;
            }

            if (Parameter(out int tokenStart, out int tokenLength))
            {
                val = new ValueToken
                {
                    TokenStart = tokenStart,
                    TokenLength = tokenLength,
                    Type = ValueTokenType.Parameter
                };
                return true;
            }
            val = null;
            return false;
        }

        internal bool Field(out FieldToken token)
        {
            var tokenStart = -1;
            var tokenLength = 0;
            var escapeChars = 0;
            var part = 0;
            var isQuoted = false;

            while (true)
            {
                if (Scanner.Identifier(beginning: part++ == 0) == false)
                {
                    if (Scanner.String())
                    {
                        isQuoted = true;
                        escapeChars += Scanner.EscapeChars;
                    }
                    else
                    {
                        token = null;
                        return false;
                    }
                }
                if (tokenStart == -1)
                    tokenStart = Scanner.TokenStart;
                tokenLength += Scanner.TokenLength;

                if (Scanner.TryScan('['))
                {
                    switch (Scanner.TryNumber())
                    {
                        case NumberToken.Long:
                        case null:
                            if (Scanner.TryScan(']') == false)
                                ThrowParseException("Expected to find closing ]");
                            tokenLength = Scanner.Position - tokenStart;
                            break;
                        case NumberToken.Double:
                            ThrowParseException("Array indexer must be integer, but got double");
                            break;
                    }
                }

                if (Scanner.TryScan('.') == false)
                    break;

                tokenLength += 1;
            }

            token = new FieldToken
            {
                EscapeChars = escapeChars,
                TokenLength = tokenLength,
                TokenStart = tokenStart,
                IsQuoted = isQuoted
            };
            return true;
        }

        private enum NextTokenOptions
        {
            Parenthesis,
            BinaryOp
        }

        public class ParseException : Exception
        {
            public ParseException(string msg) : base(msg)
            {
            }
        }
    }
}
