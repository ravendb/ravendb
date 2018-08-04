using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class StringQueryVisitor : QueryVisitor
    {
        protected readonly StringBuilder _sb;
        private int _indent;

        public StringQueryVisitor(StringBuilder sb)
        {
            _sb = sb;
        }
        
        
        public override void VisitInclude(List<QueryExpression> includes)
        {
            EnsureLine();
            _sb.Append("INCLUDE ");
            for (int i = 0; i < includes.Count; i++)
            {
                if (i != 0)
                    _sb.Append(", ");
                VisitExpression(includes[i]);
            }
            _sb.AppendLine();
        }

        public override void VisitUpdate(StringSegment update)
        {
            EnsureLine();
            _sb.Append("UPDATE { ");
            _sb.AppendLine(update.Value);
            _sb.AppendLine("}");
        }

        public override void VisitSelectFunctionBody(StringSegment func)
        {
            EnsureLine();
            _sb.AppendLine("SELECT { ");
            _sb.AppendLine(func.Value);
            _sb.AppendLine("}");
        }

        public override void VisitSelect(List<(QueryExpression Expression, StringSegment? Alias)> @select, bool isDistinct)
        {
            EnsureLine();
            _sb.Append("SELECT ");

            if (isDistinct)
                _sb.Append("DISTINCT ");
            
            VisitExpressionList(select);
            _sb.AppendLine();
        }

        private void VisitExpressionList(List<(QueryExpression Expression, StringSegment? Alias)> expressions)
        {
            for (int i = 0; i < expressions.Count; i++)
            {
                if (i != 0)
                    _sb.Append(", ");
                VisitExpression(expressions[i].Expression);
                if (expressions[i].Alias != null)
                {
                    _sb.Append(" AS ");
                    _sb.Append(expressions[i].Alias.Value);
                }
            }
        }

        public override void VisitLoad(List<(QueryExpression Expression, StringSegment? Alias)> load)
        {
            EnsureLine();
            _sb.Append("LOAD ");
            VisitExpressionList(load);
        }

        public override void VisitOrderBy(List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            EnsureLine();
            _sb.Append("ORDER BY ");
            for (int i = 0; i < orderBy.Count; i++)
            {
                if (i != 0)
                    _sb.Append(", ");
                VisitExpression(orderBy[i].Expression);
                switch (orderBy[i].FieldType)
                {
                    case OrderByFieldType.String:
                        _sb.Append(" AS string");
                        break;
                    case OrderByFieldType.Long:
                        _sb.Append(" AS long");
                        break;
                    case OrderByFieldType.Double:
                        _sb.Append(" AS double");
                        break;
                    case OrderByFieldType.AlphaNumeric:
                        _sb.Append(" AS alphanumeric");
                        break;
                }
                if (orderBy[i].Ascending == false)
                {
                    _sb.Append(" DESC");
                }
            }
        }

        public override void VisitDeclaredFunction(StringSegment name, string func)
        {
            EnsureLine();
            _sb.Append("DECLARE function ").Append(name).AppendLine(func).AppendLine();
        }

        public override void VisitWhereClause(QueryExpression where)
        {
            EnsureSpace();
            _sb.Append("WHERE ");
            base.VisitWhereClause(where);
            _sb.AppendLine();
        }

        public override void VisitNegatedExpression(NegatedExpression expr)
        {
            _sb.Append("NOT (");
            VisitExpression(expr.Expression);
            _sb.Append(")");
        }

        public override void VisitCompoundWhereExpression(BinaryExpression where)
        {
            EnsureSpace();
            _sb.Append("(");

            VisitExpression(where.Left);

            switch (where.Operator)
            {
                case OperatorType.And:
                    _sb.Append(" AND ");
                    break;
                case OperatorType.Or:
                    _sb.Append(" OR ");
                    break;
                default:
                    InvalidOperatorTypeForWhere(where);
                    break;
            }

            VisitExpression(where.Right);

            _sb.Append(")");
        }

        private static void InvalidOperatorTypeForWhere(BinaryExpression where)
        {
            throw new ArgumentOutOfRangeException("Invalid where operator type " + where.Operator);
        }

        public override void VisitMethod(MethodExpression expr)
        {
            EnsureSpace();
            _sb.Append(expr.Name.Value);
            _sb.Append("(");
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
            EnsureSpace();
            if (expr.Value == ValueTokenType.String)
                _sb.Append("'");

            if (expr.Value == ValueTokenType.Parameter)
                _sb.Append("$");
            
            _sb.Append(expr.Token.Value.Replace("'", "\\'"));

            if (expr.Value == ValueTokenType.String)
                _sb.Append("'");
        }

        public override void VisitIn(InExpression expr)
        {
            EnsureSpace();

            VisitExpression(expr.Source);

            _sb.Append(" IN (");

            for (var index = 0; index < expr.Values.Count; index++)
            {
                if (index != 0)
                    _sb.Append(", ");
                VisitExpression(expr.Values[index]);
            }

            _sb.Append(")");
        }

        public override void VisitBetween(BetweenExpression expr)
        {
            EnsureSpace();

            VisitExpression(expr.Source);

            _sb.Append(" BETWEEN ");

            VisitExpression(expr.Min);

            _sb.Append(" AND ");

            VisitExpression(expr.Max);
        }

        private void EnsureSpace()
        {
            if (_sb.Length == 0)
                return;
            var c = _sb[_sb.Length - 1];
            if (char.IsWhiteSpace(c) || c == '(')
                return;
            _sb.Append(" ");
        }

        private void EnsureLine()
        {
            if (_sb.Length != 0 && _sb[_sb.Length - 1] != '\n')
            {
                _sb.AppendLine();
            }
            for (int i = 0; i < _indent; i++)
            {
                _sb.Append("    ");
            }
        }

        public override void VisitField(FieldExpression field)
        {
            EnsureSpace();
            for (int i = 0; i < field.Compound.Count; i++)
            {
                var quote = RequiresQuotes(field.Compound[i]);

                if (quote)
                {
                    _sb.Append("'");
                    _sb.Append(field.Compound[i].Value.Replace("'", "\\'"));
                    _sb.Append("'");
                }
                else
                {
                    _sb.Append(field.Compound[i].Value);
                }
                if (i + 1 != field.Compound.Count)
                    _sb.Append(".");
            }
        }

        public override void VisitTrue()
        {
            EnsureSpace();
            _sb.Append("true");
        }

        public override void VisitSimpleWhereExpression(BinaryExpression expr)
        {
            EnsureSpace();
            VisitExpression(expr.Left);

            switch (expr.Operator)
            {
                case OperatorType.Equal:
                    _sb.Append(" = ");
                    break;
                case OperatorType.NotEqual:
                    _sb.Append(" ~= ");
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
                default:
                    InvalidOperatorTypeForWhere(expr);
                    break;
            }

            VisitExpression(expr.Right);
        }

        public override void VisitGroupByExpression(List<(QueryExpression Expression, StringSegment? Alias)> expressions)
        {
            EnsureLine();
            _sb.Append("GROUP BY ");

            VisitExpressionList(expressions);
        }

        private static bool RequiresQuotes(StringSegment s)
        {
            if (s.Length == 0)
                return true;

            var fst = s[0];
            if (char.IsLetter(fst) == false && fst != '_')
                return true;

            for (int i = 1; i < s.Length; i++)
            {
                if (char.IsLetterOrDigit(s[i]) == false)
                    return true;
            }
            return false;
        }
        
        public override void VisitFromClause(FieldExpression from, StringSegment? alias, QueryExpression filter, bool index)
        {
             EnsureLine();
            _sb.Append("FROM ");

            if (index)
                _sb.Append("INDEX ");

            VisitField(from);

            if (filter != null)
            {
                _sb.Append("(");
                
                VisitExpression(filter);
                
                _sb.Append(")");
            }
        }

        public override void VisitWithClauses(Dictionary<StringSegment, Query> expression)
        {
            foreach (var withClause in expression)
            {
                EnsureLine();
                _sb.Append("WITH {");
                _indent++;
                Visit(withClause.Value);
                _indent--;
                EnsureLine();
                _sb.Append("} AS ").Append(withClause.Key);
            }
        }

        public override void VisitWithEdgePredicates(Dictionary<StringSegment, WithEdgesExpression> expression)
        {
            foreach (var withEdgesClause in expression)
            {
                EnsureLine();
                _sb.Append("WITH EDGES(").Append(withEdgesClause.Value.EdgeType).Append(")");
                if (withEdgesClause.Value.Where == null && 
                    (withEdgesClause.Value.OrderBy == null || withEdgesClause.Value.OrderBy.Count == 0))
                {
                    _sb.Append(" AS ").Append(withEdgesClause.Key);
                    continue;
                }
                _indent++;
                EnsureLine();
                VisitWithEdgesExpression(withEdgesClause.Value);
                _indent--;
                _sb.Append("} AS ").Append(withEdgesClause.Key);
            }
        }

        public override void VisitPatternMatchElementExpression(PatternMatchElementExpression elementExpression)
        {
            EnsureSpace();
            _sb.Append(elementExpression.GetText());
        }

        public override void VisitMatchExpression(QueryExpression expr)
        {
            EnsureLine();
            _sb.Append("MATCH ");

            base.VisitMatchExpression(expr);
        }
    }
}
