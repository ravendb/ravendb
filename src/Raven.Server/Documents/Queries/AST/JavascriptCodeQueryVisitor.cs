using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public sealed class JavascriptCodeQueryVisitor : QueryVisitor
    {
        private readonly StringBuilder _sb;
        private readonly HashSet<string> _knownAliases = new HashSet<string>();
        private static readonly string[] UnsupportedQueryMethodsInJavascript = {
            "Search","Boost","Lucene","Exact","Count","Sum","Circle","Wkt","Point","Within","Contains","Disjoint","Intersects","MoreLikeThis",
            "Spatial.Wkt", "Spatial.Point", "Spatial.Intersects", "Spatial.Contains", "Spatial.Disjoint", "Spatial.Sum", "Vector.Search", "Embedding.Text",
            "Embedding.Text_I8", "Embedding.Text_I1", "Embedding.F32_I8", "Embedding.F32_I1", "Embedding.I8", "Embedding.I1", "Embedding.ForDoc"
        };

        public JavascriptCodeQueryVisitor(StringBuilder sb, Query q)
        {
            _sb = sb;

            _knownAliases.Add("this");
            if (q.From.Alias != null)
                _knownAliases.Add(q.From.Alias.Value.Value);
            if (q.Load != null)
            {
                foreach (var t in q.Load)
                {
                    _knownAliases.Add(t.Alias.Value.Value);
                }
            }

        }

        /// <summary>
        /// Rewrites '@metadata'.'@refresh' = null into "not exists(@refresh)" and
        /// '@metadata'.'@refresh' != null into "exists(@refresh)".
        /// A document with no '@refresh' at all reads as undefined in JavaScript, and
        /// 'undefined === null' is false, so the comparison has to be expressed as an
        /// existence check instead. Callers apply this to a boolean clause (a subscription
        /// where clause or a query filter clause) before visiting it.
        /// </summary>
        public static QueryExpression HandleMetadataRefresh(QueryExpression qe)
        {
            // the comparison may sit anywhere in the clause, so we recurse through the
            // logical operators and negations to reach it
            switch (qe)
            {
                case NegatedExpression ne:
                    QueryExpression inner = HandleMetadataRefresh(ne.Expression);
                    return ReferenceEquals(inner, ne.Expression) ? ne : new NegatedExpression(inner);

                case BinaryExpression { Operator: OperatorType.And or OperatorType.Or } logical:
                    QueryExpression left = HandleMetadataRefresh(logical.Left);
                    QueryExpression right = HandleMetadataRefresh(logical.Right);
                    if (ReferenceEquals(left, logical.Left) && ReferenceEquals(right, logical.Right))
                        return logical;
                    return new BinaryExpression(left, right, logical.Operator) { Parenthesis = logical.Parenthesis };

                case BinaryExpression { Operator: OperatorType.Equal or OperatorType.NotEqual } be:
                    if (IsMetadataRefreshField(be.Left) == false ||
                        be.Right is not ValueExpression { Value: ValueTokenType.Null })
                        return qe;
                    MethodExpression me = new MethodExpression("exists", [be.Left]);
                    if (be.Operator is not OperatorType.NotEqual)
                        return new NegatedExpression(me);
                    return me;

                default:
                    return qe;
            }
        }

        private static bool IsMetadataRefreshField(QueryExpression qe)
        {
            // the field may be written with or without the from alias, so we match on the
            // '@metadata'.'@refresh' suffix: both '@metadata'.'@refresh' and e.'@metadata'.'@refresh'
            if (qe is not FieldExpression fe || fe.Compound.Count < 2)
                return false;

            return fe.Compound[^1] == Constants.Documents.Metadata.Refresh &&
                   fe.Compound[^2] == Constants.Documents.Metadata.Key;
        }

        public override void VisitInclude(List<QueryExpression> includes)
        {
           throw new NotSupportedException();
        }

        public override void VisitUpdate(StringSegment update)
        {
            throw new NotSupportedException();
        }

        public override void VisitSelectFunctionBody(StringSegment func)
        {
            throw new NotSupportedException();
        }

        public override void VisitSelect(List<(QueryExpression Expression, StringSegment? Alias)> select, bool isDistinct)
        {
            throw new NotSupportedException();
        }

        public override void VisitSelectDistinct()
        {
            throw new NotSupportedException();
        }

        public override void VisitLoad(List<(QueryExpression Expression, StringSegment? Alias)> load)
        {
            throw new NotSupportedException();
        }

        public override void VisitOrderBy(List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending, NullsOrderingType NullsOrdering)> orderBy)
        {
            throw new NotSupportedException();
        }

        public override void VisitDeclaredFunction(string func)
        {
            throw new NotSupportedException();
        }

        public override void VisitNegatedExpression(NegatedExpression expr)
        {
            _sb.Append("!(");
            VisitExpression(expr.Expression);
            _sb.Append(")");
        }

        public override void VisitCompoundWhereExpression(BinaryExpression @where)
        {
            _sb.Append("(");

            VisitExpression(where.Left);

            switch (where.Operator)
            {
                case OperatorType.And:
                    _sb.Append(" && ");
                    break;
                case OperatorType.Or:
                    _sb.Append(" || ");
                    break;
            }

            
            VisitExpression(where.Right);
            
            _sb.Append(")");
        }

        private static void AssertMethodSupported(MethodExpression expr)
        {
            if (expr.Name.Value.Equals("now", StringComparison.OrdinalIgnoreCase) ||
                expr.Name.Value.Equals("today", StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException($"'{expr.Name.Value}()' function is not supported in filter or subscription expressions");
            }
        }

        public override void VisitMethod(MethodExpression expr)
        {
            AssertMethodSupported(expr);

            if (expr.Name.Value.Equals("startswith", StringComparison.OrdinalIgnoreCase))
            {
                if (expr.Arguments.Count != 2)
                {
                    throw new InvalidOperationException("startsWith(text, prefix) must be called with two string parameters");
                }
                _sb.Append("startsWith(");
                VisitExpression(expr.Arguments[0]);
                _sb.Append(",");
                VisitExpression(expr.Arguments[1]);
                _sb.Append(")");
                return;
            }
            
            if (expr.Name.Value.Equals("endswith", StringComparison.OrdinalIgnoreCase))
            {
                if (expr.Arguments.Count != 2)
                {
                    throw new InvalidOperationException("endsWith(text, suffix) must be called with two string parameters");
                }
                _sb.Append("endsWith(");
                VisitExpression(expr.Arguments[0]);
                _sb.Append(",");
                VisitExpression(expr.Arguments[1]);
                _sb.Append(")");
                return;
            }

            if (expr.Name.Value.Equals("regex", StringComparison.OrdinalIgnoreCase))
            {
                if (expr.Arguments.Count != 2)
                {
                    throw new InvalidOperationException("regex(text, regex) must be called with two string parameters");
                }
                _sb.Append("regex(");
                VisitExpression(expr.Arguments[0]);
                _sb.Append(",");
                VisitExpression(expr.Arguments[1]);
                _sb.Append(")");
                return;
            }

            if (expr.Name.Value.Equals("intersect", StringComparison.OrdinalIgnoreCase))
            {
                if (expr.Arguments.Count < 2)
                {
                    throw new InvalidOperationException("intersect(logical statement, logical statement, ..) must be called with two or more logical statements parameters");
                }
                _sb.Append("(");
                for (var index = 0; index < expr.Arguments.Count; index++)
                {
                    var argument = expr.Arguments[index];
                    
                    VisitExpression(argument);
                    
                    if (index < expr.Arguments.Count - 1)
                        _sb.Append(" && ");
                }
                
                _sb.Append(")");
                return;    
            }
            
            if (expr.Name.Value.Equals("exists", StringComparison.OrdinalIgnoreCase))
            {
                if (expr.Arguments.Count != 1)
                {
                    throw new InvalidOperationException("exists(field name) must be called with one string parameter");
                }
                _sb.Append("(typeof "); 
                VisitExpression(expr.Arguments[0]);
                _sb.Append("!== 'undefined')");
                return;    
            }
            
            if (UnsupportedQueryMethodsInJavascript.Any(x=>
                x.Equals(expr.Name.Value, StringComparison.OrdinalIgnoreCase)))
            {
                throw new NotSupportedException($"'{expr.Name.Value}' query method is not supported for this type of query");
            }
            
            _sb.Append(expr.Name.Value);
            _sb.Append("(");

            
            if (expr.Name.Value == "id" && expr.Arguments.Count == 0)
            {
                _sb.Append("this");
            }

            for (var index = 0; index < expr.Arguments.Count; index++)
            {
                if (index != 0)
                    _sb.Append(", ");
                VisitExpression(expr.Arguments[index]);
            }
            _sb.Append(")");
        }

        public override void VisitValue(ValueExpression expr)
        {
            if (expr.Value == ValueTokenType.String)
                _sb.Append('"');

            if (expr.Value == ValueTokenType.Parameter)
                _sb.Append('$');
            
            _sb.Append(expr.Token.Value.Replace("\\", "\\\\"));

            if (expr.Value == ValueTokenType.String)
                _sb.Append('"');
        }

        public override void VisitIn(InExpression expr)
        {
            _sb.Append("[");

            for (var index = 0; index < expr.Values.Count; index++)
            {
                if(index != 0)
                    _sb.Append(", ");
                VisitExpression(expr.Values[index]);
            }

            _sb.Append("].indexOf(");
            VisitExpression(expr.Source);
            _sb.Append(") >= 0");
        }

        public override void VisitBetween(BetweenExpression expr)
        {
            _sb.Append(" between( ");
            VisitExpression(expr.Source);
            _sb.Append(", ");
            VisitExpression(expr.Min);
            _sb.Append(", ");
            VisitExpression(expr.Max);
            _sb.Append(")");
        }

        public override void VisitField(FieldExpression field)
        {
            if (_knownAliases.Contains(field.Compound[0].Value) == false)
                _sb.Append("this.");

            for (int i = 0; i < field.Compound.Count; i++)
            {
                EscapeFieldName(_sb, field.Compound[i].Value);
                if (i + 1 != field.Compound.Count)
                    _sb.Append(".");
            }
        }

        private static void EscapeFieldName(StringBuilder sb, string name)
        {
            if (name[0] == '_' || char.IsLetter(name[0]))
            {
                var valid = true;
                for (int i = 1; i < name.Length; i++)
                {
                    valid &= name[i] == '_' || char.IsLetterOrDigit(name[i]);
                }

                if (valid)
                {
                    sb.Append(name);
                    return;
                }
            }

            if (sb.Length > 0 && sb[^1] == '.')
                sb.Length--;

            sb.Append("['").Append(name.Replace("'", "\\'")).Append("']");
        }

        public override void VisitTrue()
        {
            _sb.Append("true");
        }

        public override void VisitSimpleWhereExpression(BinaryExpression expr)
        {
            VisitExpression(expr.Left);

            switch (expr.Operator)
            {
                case OperatorType.Equal:
                    _sb.Append(" === ");
                    break;
                case OperatorType.NotEqual:
                    _sb.Append(" !== ");
                    break;
                case OperatorType.LessThan:
                    _sb.Append(" < ");
                    break;
                case OperatorType.GreaterThan:
                    _sb.Append(" > ");
                    break;
                case OperatorType.LessThanEqual:
                    _sb.Append(" <= ");
                    break;
                case OperatorType.GreaterThanEqual:
                    _sb.Append(" >= ");
                    break;
            }

            VisitExpression(expr.Right);
        }

        public override void VisitGroupByExpression(List<(QueryExpression Expression, StringSegment? Alias)> expressions)
        {
            throw new NotSupportedException();
        }

        public override void VisitFromClause(FieldExpression @from, StringSegment? alias, QueryExpression filter, bool index)
        {
            throw new NotSupportedException();
        }
    }
}
