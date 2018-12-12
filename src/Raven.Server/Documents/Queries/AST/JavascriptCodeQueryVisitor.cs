using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Extensions.Primitives;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class JavascriptCodeQueryVisitor : QueryVisitor
    {
        private readonly StringBuilder _sb;
        private readonly HashSet<string> _knownAliases = new HashSet<string>();
        private static readonly string[] UnsupportedQueryMethodsInJavascript = {
            "Search","Boost","Lucene","Exact","Count","Sum","Circle","Wkt","Point","Within","Contains","Disjoint","Intersects","MoreLikeThis"
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

        public override void VisitLoad(List<(QueryExpression Expression, StringSegment? Alias)> load)
        {
            throw new NotSupportedException();
        }

        public override void VisitOrderBy(List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            throw new NotSupportedException();
        }

        public override void VisitDeclaredFunctions(Dictionary<StringSegment, (string FunctionText, Esprima.Ast.Program Program)> declaredFunctions)
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

        public override void VisitMethod(MethodExpression expr)
        {
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
                throw new NotSupportedException($"'{expr.Name.Value}' query method is not supported by subscriptions");
            }
            
            _sb.Append(expr.Name.Value);
            _sb.Append("(");

            
            if (expr.Name.Value == "id" && expr.Arguments.Count == 0)
            {
                if (expr.Arguments.Count != 1)
                {
                    throw new InvalidOperationException("id(document) must be called with one document parameter");
                }
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
            
            _sb.Append(expr.Token.Value.Replace("\\", "\\\\"));

            if (expr.Value == ValueTokenType.String)
                _sb.Append('"');
        }

        public override void VisitIn(InExpression expr)
        {
            _sb.Append(" in(");
            VisitExpression(expr.Source);

            for (var index = 0; index < expr.Values.Count; index++)
            {
                _sb.Append(", ");
                VisitExpression(expr.Values[index]);
            }

            _sb.Append(")");
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
            if(_knownAliases.Contains(field.Compound[0].Value) == false)
                _sb.Append("this.");

            for (int i = 0; i < field.Compound.Count; i++)
            {
                _sb.Append(field.Compound[i].Value);
                if (i + 1 != field.Compound.Count)
                    _sb.Append(".");
            }
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

        public override void VisitDeclaredFunction(StringSegment name, string func)
        {
            throw new NotSupportedException();
        }
    }
}
