using System;
using System.Collections.Generic;
using Microsoft.Extensions.Primitives;
using Newtonsoft.Json;

namespace Raven.Server.Documents.Queries.AST
{
    public class JsonQueryVisitor : QueryVisitor
    {
        private JsonWriter _writer;

        public JsonQueryVisitor(JsonWriter writer)
        {
            _writer = writer;
        }

        public override void VisitNegatedExpression(NegatedExpression expr)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteValue("Not");
            _writer.WritePropertyName("Expression");
            VisitExpression(expr.Expression);
            _writer.WriteEndObject();
        }

        public override void VisitInclude(List<QueryExpression> includes)
        {
            _writer.WritePropertyName("Include");
            _writer.WriteStartArray();
            foreach (var include in includes)
            {
                VisitExpression(include);
            }
            _writer.WriteEndArray();
        }

        public override void VisitUpdate(StringSegment update)
        {
            _writer.WritePropertyName("Update");
            _writer.WriteValue(update.Value);
        }

        public override void VisitSelectFunctionBody(StringSegment func)
        {
            _writer.WritePropertyName("Select");
            _writer.WriteValue(func.Value);
        }

        public override void VisitSelect(List<(QueryExpression Expression, StringSegment? Alias)> select, bool isDistinct)
        {
            _writer.WritePropertyName("Select");
            WriteExpressionList(@select);
            
            if (isDistinct)
            {
                _writer.WritePropertyName("IsDistinct");
                _writer.WriteValue(true);
            }
        }

        private void WriteExpressionList(List<(QueryExpression Expression, StringSegment? Alias)>  expressions)
        {
            _writer.WriteStartArray();
            foreach (var expr in expressions)
            {
                _writer.WriteStartObject();
                _writer.WritePropertyName("Expression");
                VisitExpression(expr.Expression);
                if (expr.Alias != null)
                {
                    _writer.WritePropertyName("Alias");
                    _writer.WriteValue((string)expr.Alias.Value.Value);
                }
                _writer.WriteEndObject();
            }
            _writer.WriteEndArray();
        }

        public override void VisitLoad(List<(QueryExpression Expression, StringSegment? Alias)> load)
        {
            _writer.WritePropertyName("Load");
            WriteExpressionList(load);
        }

        public override void VisitOrderBy(List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> orderBy)
        {
            _writer.WritePropertyName("OrderBy");
            _writer.WriteStartArray();
            foreach (var field in orderBy)
            {
                _writer.WriteStartObject();
                _writer.WritePropertyName("Field");
                
                VisitExpression(field.Expression);

                if (field.FieldType != OrderByFieldType.Implicit)
                {
                    _writer.WritePropertyName("FieldType");
                    _writer.WriteValue(field.FieldType.ToString());
                }

                _writer.WritePropertyName("Ascending");
                _writer.WriteValue(field.Ascending);
                _writer.WriteEndObject();
            }
            _writer.WriteEndArray();
        }

        public override void VisitDeclaredFunctions(Dictionary<StringSegment, (string FunctionText, Esprima.Ast.Program Program)> declaredFunctions)
        {
            _writer.WritePropertyName("Declare");
            _writer.WriteStartObject();
            foreach (var declaredFunction in declaredFunctions)
            {
                _writer.WritePropertyName(declaredFunction.Key.Value);
                _writer.WriteValue(declaredFunction.Value.FunctionText);
            }
            _writer.WriteEndObject();
        }

        public override void VisitWhereClause(QueryExpression @where)
        {
            _writer.WritePropertyName("Where");
            base.VisitWhereClause(where);
        }

        public override void VisitCompoundWhereExpression(BinaryExpression @where)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Type");
            _writer.WriteValue(where.Operator.ToString());
            
            _writer.WritePropertyName("Left");
            VisitExpression(where.Left);

            _writer.WritePropertyName("Right");
            VisitExpression(where.Right);

            _writer.WriteEndObject();
        }

        public override void VisitMethod(MethodExpression expr)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Method");
            _writer.WriteValue(expr.Name.Value);
            _writer.WritePropertyName("Arguments");
            _writer.WriteStartArray();
            foreach (var arg in expr.Arguments)
            {
                VisitExpression(arg);
            }
            _writer.WriteEndArray();
            _writer.WriteEndObject();
        }

        public override void VisitValue(ValueExpression expr)
        {
            switch (expr.Value)
            {
                case ValueTokenType.Parameter:
                    _writer.WriteStartObject();
                    _writer.WritePropertyName("Parameter");
                    _writer.WriteValue(expr.Token.Value);
                    _writer.WriteEndObject();
                    break;
                case ValueTokenType.Long:
                    _writer.WriteValue(QueryBuilder.ParseInt64WithSeparators(expr.Token.Value));
                    break;
                case ValueTokenType.Double:
                    _writer.WriteRawValue(expr.Token.Value);
                    break;
                case ValueTokenType.String:
                    _writer.WriteValue(expr.Token.Value);
                    break;
                case ValueTokenType.True:
                    _writer.WriteValue(true);
                    break;
                case ValueTokenType.False:
                    _writer.WriteValue(false);
                    break;
                case ValueTokenType.Null:
                    _writer.WriteNull();
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public override void VisitIn(InExpression expr)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName(expr.All ? "AllIn": "In");
            _writer.WriteStartArray();
            foreach (var value in expr.Values)
            {
                VisitExpression(value);
            }
            _writer.WriteEndArray();
            _writer.WriteEndObject();
        }

        public override void VisitBetween(BetweenExpression expr)
        {
            _writer.WriteStartObject();
            _writer.WritePropertyName("Between");
            _writer.WriteStartObject();
            _writer.WritePropertyName("Min");
            VisitExpression(expr.Min);
            _writer.WritePropertyName("Max");
            VisitExpression(expr.Max);
            _writer.WriteEndObject();
            _writer.WriteEndObject();
        }

        public override void VisitField(FieldExpression field)
        {
            _writer.WriteValue(field.FieldValue);
        }

        public override void VisitTrue()
        {
            _writer.WriteValue(true);
        }

        public override void VisitSimpleWhereExpression(BinaryExpression expr)
        {
            VisitCompoundWhereExpression(expr);
        }

        public override void VisitGroupByExpression(List<(QueryExpression Expression, StringSegment? Alias)> expressions)
        {
            _writer.WritePropertyName("GroupBy");
            WriteExpressionList(expressions);
        }

        public override void VisitFromClause(FieldExpression from, StringSegment? alias, QueryExpression filter, bool index)
        {
            _writer.WritePropertyName("From");
            VisitField(from);
            if (index)
            {
                _writer.WritePropertyName("Index");
                _writer.WriteValue(true);
            }
            if (alias != null)
            {
                _writer.WritePropertyName("Alias");
                _writer.WriteValue(alias.Value.Value);
            }
        }

    }
}
