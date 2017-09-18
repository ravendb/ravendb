using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class StringQueryVisitor : QueryVisitor
    {
        StringBuilder _sb = new StringBuilder();
        
        public override void VisitInclude(List<QueryExpression> includes)
        {
            _sb.AppendLine("INCLUDE ");
            for (int i = 0; i < includes.Count; i++)
            {
                if (i != 0)
                    _sb.Append(", ");
                VisitExpression(includes[i]);
            }
        }

        public override void VisitUpdate(StringSegment update)
        {
            _sb.AppendLine("UPDATE { ");
            _sb.AppendLine(update.Value);
            _sb.Append("}");
        }

        public override void VisitSelectFunctionBody(StringSegment func)
        {
            _sb.AppendLine("SELECT { ");
            _sb.AppendLine(func.Value);
            _sb.Append("}");
        }

        public override void VisitSelect(List<(QueryExpression Expression, StringSegment? Alias)> select)
        {
            _sb.AppendLine("SELECT ");
            VisitExpressionList(@select);
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
                    _sb.Append((string)expressions[i].Alias.Value);
                }
            }
        }

        public override void VisitLoad(List<(QueryExpression Expression, StringSegment? Alias)> load)
        {
            _sb.AppendLine("LOAD ");
            VisitExpressionList(load);
      
        }

        public override void VisitOrderBy(List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            _sb.AppendLine("ORDER BY ");
            for (int i = 0; i < orderBy.Count; i++)
            {
                if (i != 0)
                    _sb.Append(", ");
                VisitExpression(orderBy[i].Expression);
                switch (orderBy[i].FieldType)
                {
                    case OrderByFieldType.String:
                        _sb.Append(" AS string")
                        break;
                    case OrderByFieldType.Long:
                        _sb.Append(" AS long")
                        break;
                    case OrderByFieldType.Double:
                        _sb.Append(" AS double")
                        break;
                    case OrderByFieldType.AlphaNumeric:
                        _sb.Append(" AS alphanumeric")
                        break;
                }
                if (orderBy[i].Ascending == false)
                {
                    _sb.Append(" DESC");
                }
            }
        }

        private static void ThrowInvalidFieldType()
        {
            throw new ArgumentOutOfRangeException();
        }

        public override void VisitDeclaredFunctions(Dictionary<StringSegment, StringSegment> declaredFunctions)
        {
            foreach (var kvp in declaredFunctions)
            {
                _sb.Append("DECLARE FUNCTION ").Append(kvp.Key).AppendLine(kvp.Value.Value).AppendLine();
            }
        }

        public override void VisitWhereClause(BinaryExpression @where)
        {
            base.VisitWhereClause(@where);
        }

        public override void VisitCompoundWhereExpression(BinaryExpression @where)
        {
            base.VisitCompoundWhereExpression(@where);
        }

        public override void VisitMethod(MethodExpression expr)
        {
            base.VisitMethod(expr);
        }

        public override void VisitValue(ValueExpression expr)
        {
            base.VisitValue(expr);
        }

        public override void VisitIn(InExpression expr)
        {
            base.VisitIn(expr);
        }

        public override void VisitBetween(BetweenExpression expr)
        {
            base.VisitBetween(expr);
        }

        public override void VisitField(FieldExpression field)
        {
            base.VisitField(field);
        }

        public override void VisitTrue()
        {
            base.VisitTrue();
        }

        public override void VisitSimpleWhereExpression(BinaryExpression expr)
        {
            base.VisitSimpleWhereExpression(expr);
        }

        public override void VisitGroupByExpression(List<FieldExpression> expressions)
        {
            base.VisitGroupByExpression(expressions);
        }

        public override void VisitFromClause(ref (FieldExpression From, Nullable<StringSegment>? Alias, QueryExpression Filter, bool Index) @from, bool isDistinct)
        {
            base.VisitFromClause(ref @from, isDistinct);
        }

        public override void VisitDeclaredFunction(StringSegment name, StringSegment fund)
        {
            base.VisitDeclaredFunction(name, fund);
        }

        public override void VisitWhere(QueryExpression @where)
        {
            base.VisitWhere(@where);
        }

        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public override string ToString()
        {
            return base.ToString();
        }
    }
}
