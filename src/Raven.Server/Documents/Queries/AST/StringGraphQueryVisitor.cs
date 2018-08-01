using System;
using System.Collections.Generic;
using System.Text;
using JetBrains.Annotations;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class StringGraphQueryVisitor : GraphQueryVisitor
    {
        [NotNull]
        protected readonly StringBuilder Sb;

        public StringGraphQueryVisitor([NotNull] StringBuilder sb)
        {
            Sb = sb ?? throw new ArgumentNullException(nameof(sb));
        }

        public override void VisitWithClauses(Dictionary<StringSegment, Query> expression)
        {
            foreach (var withClause in expression)
            {
                Sb.Append("WITH { ");
                    Visit(withClause.Value);
                Sb.AppendLine($" }} AS {withClause.Key}");
            }
        }

        private bool _hasOutputMatch;
        public override void VisitPatternMatchClause(PatternMatchExpression expression)
        {
            if (!_hasOutputMatch)
            {
                Sb.Append("MATCH ");
                _hasOutputMatch = true;
            }

            base.VisitPatternMatchClause(expression);
        }

        public override void VisitWithEdgePredicates(Dictionary<StringSegment, WithEdgesExpression> expression)
        {
            foreach (var withEdgesClause in expression)
            {
                Sb.Append($"WITH EDGES({withEdgesClause.Value.EdgeType}) {{ ");
                    VisitWithEdgesExpression(withEdgesClause.Value);
                Sb.AppendLine($" }} AS {withEdgesClause.Key}");
            }
        }

        public override void VisitBinaryExpression(PatternMatchBinaryExpression binaryExpression)
        {
            Sb.Append("("); 
                VisitPatternMatchClause(binaryExpression.Left);
            Sb.Append(")");
            
            VisitBinaryOperator(binaryExpression, binaryExpression.Op);
            
            Sb.Append("("); 
                VisitPatternMatchClause(binaryExpression.Right);
            Sb.Append(")");

            Sb.AppendLine();
        }
        
        public override void VisitElementExpression(PatternMatchElementExpression elementExpression)
        {
            Sb.Append($" {elementExpression} ");
        }

        public override void VisitNotExpression(PatternMatchNotExpression notExpression)
        {
            Sb.Append("NOT (");
            base.VisitNotExpression(notExpression);
            Sb.Append(")");
        }

        public override void VisitBinaryOperator(PatternMatchBinaryExpression binaryExpression, PatternMatchBinaryExpression.Operator op)
        {
            Sb.Append($" {op} ");
        }

        public override void VisitInclude(List<QueryExpression> includes)
        {
            EnsureLine();
            Sb.Append("INCLUDE ");
            for (int i = 0; i < includes.Count; i++)
            {
                if (i != 0)
                    Sb.Append(", ");
                VisitExpression(includes[i]);
            }
            Sb.AppendLine();
        }

        public override void VisitUpdate(StringSegment update)
        {
            EnsureLine();
            Sb.Append("UPDATE { ");
            Sb.AppendLine(update.Value);
            Sb.AppendLine("}");
        }

        public override void VisitSelectFunctionBody(StringSegment func)
        {
            EnsureLine();
            Sb.AppendLine("SELECT { ");
            Sb.AppendLine(func.Value);
            Sb.AppendLine("}");
        }

        public override void VisitSelect(List<(QueryExpression Expression, StringSegment? Alias)> @select, bool isDistinct)
        {
            EnsureLine();
            Sb.Append("SELECT ");

            if (isDistinct)
                Sb.Append("DISTINCT ");
            
            VisitExpressionList(select);
            Sb.AppendLine();
        }

        private void VisitExpressionList(List<(QueryExpression Expression, StringSegment? Alias)> expressions)
        {
            for (int i = 0; i < expressions.Count; i++)
            {
                if (i != 0)
                    Sb.Append(", ");
                VisitExpression(expressions[i].Expression);
                if (expressions[i].Alias != null)
                {
                    Sb.Append(" AS ");
                    Sb.Append(expressions[i].Alias.Value);
                }
            }
        }

        public override void VisitLoad(List<(QueryExpression Expression, StringSegment? Alias)> load)
        {
            EnsureLine();
            Sb.Append("LOAD ");
            VisitExpressionList(load);
        }

        public override void VisitOrderBy(List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            EnsureLine();
            Sb.Append("ORDER BY ");
            for (int i = 0; i < orderBy.Count; i++)
            {
                if (i != 0)
                    Sb.Append(", ");
                VisitExpression(orderBy[i].Expression);
                switch (orderBy[i].FieldType)
                {
                    case OrderByFieldType.String:
                        Sb.Append(" AS string");
                        break;
                    case OrderByFieldType.Long:
                        Sb.Append(" AS long");
                        break;
                    case OrderByFieldType.Double:
                        Sb.Append(" AS double");
                        break;
                    case OrderByFieldType.AlphaNumeric:
                        Sb.Append(" AS alphanumeric");
                        break;
                }
                if (orderBy[i].Ascending == false)
                {
                    Sb.Append(" DESC");
                }
            }
        }

        public override void VisitDeclaredFunction(StringSegment name, string func)
        {
            EnsureLine();
            Sb.Append("DECLARE function ").Append(name).AppendLine(func).AppendLine();
        }

        public override void VisitWhereClause(QueryExpression where)
        {
            EnsureSpace();
            Sb.Append("WHERE ");
            base.VisitWhereClause(where);
            Sb.AppendLine();
        }

        public override void VisitNegatedExpresson(NegatedExpression expr)
        {
            Sb.Append("NOT (");
            VisitExpression(expr.Expression);
            Sb.Append(")");
        }

        public override void VisitCompoundWhereExpression(BinaryExpression where)
        {
            EnsureSpace();
            Sb.Append("(");

            VisitExpression(where.Left);

            switch (where.Operator)
            {
                case OperatorType.And:
                    Sb.Append(" AND ");
                    break;
                case OperatorType.Or:
                    Sb.Append(" OR ");
                    break;
                default:
                    InvalidOperatorTypeForWhere(where);
                    break;
            }

            VisitExpression(where.Right);

            Sb.Append(")");
        }

        private static void InvalidOperatorTypeForWhere(BinaryExpression where)
        {
            throw new ArgumentOutOfRangeException("Invalid where operator type " + where.Operator);
        }

        public override void VisitMethod(MethodExpression expr)
        {
            EnsureSpace();
            Sb.Append(expr.Name.Value);
            Sb.Append("(");
            for (var index = 0; index < expr.Arguments.Count; index++)
            {
                if (index != 0)
                    Sb.Append(", ");
                VisitExpression(expr.Arguments[index]);
            }
            Sb.Append(")");
        }

        public override void VisitValue(ValueExpression expr)
        {
            EnsureSpace();
            if (expr.Value == ValueTokenType.String)
                Sb.Append("'");

            if (expr.Value == ValueTokenType.Parameter)
                Sb.Append("$");
            
            Sb.Append(expr.Token.Value.Replace("'", "\\'"));

            if (expr.Value == ValueTokenType.String)
                Sb.Append("'");
        }

        public override void VisitIn(InExpression expr)
        {
            EnsureSpace();

            VisitExpression(expr.Source);

            Sb.Append(" IN (");

            for (var index = 0; index < expr.Values.Count; index++)
            {
                if (index != 0)
                    Sb.Append(", ");
                VisitExpression(expr.Values[index]);
            }

            Sb.Append(")");
        }

        public override void VisitBetween(BetweenExpression expr)
        {
            EnsureSpace();

            VisitExpression(expr.Source);

            Sb.Append(" BETWEEN ");

            VisitExpression(expr.Min);

            Sb.Append(" AND ");

            VisitExpression(expr.Max);
        }

        private void EnsureSpace()
        {
            if (Sb.Length == 0)
                return;
            var c = Sb[Sb.Length - 1];
            if (char.IsWhiteSpace(c) || c == '(')
                return;
            Sb.Append(" ");
        }

        private void EnsureLine()
        {
            if (Sb.Length == 0)
                return;
            if (Sb[Sb.Length - 1] == '\n')
                return;
            
            Sb.AppendLine();
        }

        public override void VisitField(FieldExpression field)
        {
            EnsureSpace();
            for (int i = 0; i < field.Compound.Count; i++)
            {
                var quote = RequiresQuotes(field.Compound[i]);

                if (quote)
                {
                    Sb.Append("'");
                    Sb.Append(field.Compound[i].Value.Replace("'", "\\'"));
                    Sb.Append("'");
                }
                else
                {
                    Sb.Append(field.Compound[i].Value);
                }
                if (i + 1 != field.Compound.Count)
                    Sb.Append(".");
            }
        }

        public override void VisitTrue()
        {
            EnsureSpace();
            Sb.Append("true");
        }

        public override void VisitSimpleWhereExpression(BinaryExpression expr)
        {
            EnsureSpace();
            VisitExpression(expr.Left);

            switch (expr.Operator)
            {
                case OperatorType.Equal:
                    Sb.Append(" = ");
                    break;
                case OperatorType.NotEqual:
                    Sb.Append(" ~= ");
                    break;
                case OperatorType.LessThan:
                    Sb.Append(" < ");
                    break;
                case OperatorType.GreaterThan:
                    Sb.Append(" > ");
                    break;
                case OperatorType.LessThanEqual:
                    Sb.Append(" <= ");
                    break;
                case OperatorType.GreaterThanEqual:
                    Sb.Append(" >= ");
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
            Sb.Append("GROUP BY ");

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
            Sb.Append("FROM ");

            if (index)
                Sb.Append("INDEX ");

            VisitField(from);

            if (filter != null)
            {
                Sb.Append("(");
                
                VisitExpression(filter);
                
                Sb.Append(")");
            }
        }

    }
}
