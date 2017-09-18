using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public class QueryExpression
    {
        public FieldToken Field;
        public ValueToken First, Second; // between

        public QueryExpression Left;
        public QueryExpression Right;
        public OperatorType Type;

        public ValueToken Value; // =, <, >, <=, >=
        public List<ValueToken> Values; // in 
        public List<object> Arguments; // method

        [ThreadStatic]
        private static StringBuilder _tempBuffer;

        internal static string Extract(ValueToken val, bool stripQuotes = false)
        {
            switch (val.Type)
            {
                case ValueTokenType.False:
                    return "false";
                case ValueTokenType.Null:
                    return "null";
                case ValueTokenType.True:
                    return "true";
            }
            if (stripQuotes && val.Type == ValueTokenType.String)
                return val.Token.Subsegment(1, val.Token.Length - 2).ToString();

            return val.Token.ToString();
        }

        internal static string Extract(FieldToken field)
        {
            if (field.IsQuoted)
                return field.Token.Subsegment(1, field.Token.Length - 2).ToString();
            return field.Token.ToString();
        }

        internal static string Extract(StringSegment token, int escapeChars)
        {
            return token.ToString();
        }

        internal static void WriteValue(JsonWriter writer, ValueToken token,
            bool raw = false)
        {
            if (token.EscapeChars == 0)
            {
                if (token.Token.Length != 0)
                {
                    if (token.Token[0] == '"' || token.Token[0] == '\'')
                    {
                        // skip quotes
                        writer.WriteValue(token.Token.Subsegment(1, token.Token.Length-2));
                        return;
                    }
                }
                if (raw)
                    writer.WriteRawValue(token.Token.ToString());
                else
                    writer.WriteValue(token.Token.ToString());
                return;
            }
            writer.WriteValue(Extract(token));
        }
        
        internal static void WriteValue(JsonWriter writer, FieldToken token,
            bool raw = false)
        {
            if (token.EscapeChars == 0)
            {
                if (token.Token.Length != 0)
                {
                    if (token.Token[0] == '"' || token.Token[0] == '\'')
                    {
                        // skip quotes
                        writer.WriteValue(token.Token.Subsegment(1, token.Token.Length-2));
                        return;
                    }
                }
                if (raw)
                    writer.WriteRawValue(token.Token.ToString());
                else
                    writer.WriteValue(token.Token.ToString());
                return;
            }
            writer.WriteValue(Extract(token));
        }

        public void ToJavaScript(string query, string alias, TextWriter writer)
        {
            switch (Type)
            {
                case OperatorType.Field:
                    if (alias != null)
                    {
                        writer.Write(alias);
                        writer.Write(".");
                    }
                    writer.Write(Extract(Field));
                    break;
                case OperatorType.Equal:
                case OperatorType.NotEqual:
                case OperatorType.LessThan:
                case OperatorType.GreaterThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:
                    var fieldName = Extract(Field);
                    WriteSimpleOperatorJavaScript(query, writer, fieldName, alias, Type, Value);
                    break;
                case OperatorType.Between:
                    throw new InvalidOperationException("Cannot translate between operation to JavaScript");
                case OperatorType.In:
                case OperatorType.AllIn:
                    throw new InvalidOperationException("Cannot translate in operation to JavaScript");
                case OperatorType.And:
                case OperatorType.AndNot:
                case OperatorType.Or:
                case OperatorType.OrNot:
                    writer.Write("(");
                    Left.ToJavaScript(query, alias, writer);
                    switch (Type)
                    {
                        case OperatorType.And:
                            writer.Write(" && ");
                            break;
                        case OperatorType.AndNot:
                            writer.Write(" && !(");
                            break;
                        case OperatorType.Or:
                            writer.Write(" || ");
                            break;
                        case OperatorType.OrNot:
                            writer.Write(" || !(");
                            break;
                    }
                    Right.ToJavaScript(query, alias, writer);
                    if (Type == OperatorType.OrNot || Type == OperatorType.AndNot)
                    {
                        writer.Write(")");
                    }
                    writer.Write(")");
                    break;
                case OperatorType.Method:
                    var method = Extract(Field);
                    var methodType = QueryMethod.GetMethodType(method, throwIfNoMatch: false);
                    if (methodType == MethodType.Id && Arguments.Count == 1 && Arguments[0] is QueryExpression idExpression)
                    {
                        WriteSimpleOperatorJavaScript(query, writer, $"id({alias})", null, idExpression.Type, idExpression.Value);
                        break;
                    }

                    writer.Write(method);
                    writer.Write("(");

                    for (int i = 0; i < Arguments.Count; i++)
                    {
                        var arg = Arguments[i];
                        if (i != 0)
                            writer.Write(", ");
                        if (arg is QueryExpression qe)
                        {
                            qe.ToJavaScript(query, alias, writer);
                        }
                        else if (arg is FieldToken field)
                        {
                            writer.Write(Extract(field));
                        }
                        else
                        {
                            var val = (ValueToken)arg;
                            writer.Write(Extract(val));
                        }
                    }
                    writer.Write(")");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static void WriteSimpleOperatorJavaScript(string query, TextWriter writer, string fieldName, string alias, OperatorType type, ValueToken value)
        {
            if (alias != null)
            {
                writer.Write(alias);
                writer.Write(".");
            }
            writer.Write(fieldName);
            switch (type)
            {
                case OperatorType.Equal:
                    writer.Write(" == ");
                    break;
                case OperatorType.NotEqual:
                    writer.Write(" != ");
                    break;
                case OperatorType.LessThan:
                    writer.Write(" < ");
                    break;
                case OperatorType.GreaterThan:
                    writer.Write(" > ");
                    break;
                case OperatorType.LessThanEqual:
                    writer.Write(" <= ");
                    break;
                case OperatorType.GreaterThanEqual:
                    writer.Write(" >= ");
                    break;
                default:
                    ThrowInvalidType(type);
                    break;
            }
            writer.Write(Extract(value));
        }

        public void ToString(string query, TextWriter writer)
        {
            switch (Type)
            {
                case OperatorType.Field:
                    writer.Write(Extract(Field));
                    writer.Write(" ");
                    break;
                case OperatorType.Equal:
                case OperatorType.NotEqual:
                case OperatorType.LessThan:
                case OperatorType.GreaterThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:
                    writer.Write(Extract(Field));
                    switch (Type)
                    {
                        case OperatorType.Equal:
                            writer.Write(" = ");
                            break;
                        case OperatorType.NotEqual:
                            writer.Write(" != ");
                            break;
                        case OperatorType.LessThan:
                            writer.Write(" < ");
                            break;
                        case OperatorType.GreaterThan:
                            writer.Write(" > ");
                            break;
                        case OperatorType.LessThanEqual:
                            writer.Write(" <= ");
                            break;
                        case OperatorType.GreaterThanEqual:
                            writer.Write(" >= ");
                            break;
                        default:
                            ThrowInvalidType(Type);
                            break;
                    }
                    writer.Write(Extract(Value));
                    break;
                case OperatorType.Between:
                    writer.Write(Extract(Field));
                    writer.Write(" BETWEEN ");
                    writer.Write(Extract(First));
                    writer.Write(" AND ");
                    writer.Write(Extract(Second));
                    break;
                case OperatorType.In:
                case OperatorType.AllIn:
                    writer.Write(Extract(Field));
                    writer.Write(Type != OperatorType.AllIn ? " IN (" : " ALL IN (");
                    for (var i = 0; i < Values.Count; i++)
                    {
                        var value = Values[i];
                        if (i != 0)
                            writer.Write(", ");
                        writer.Write(Extract(value));
                    }
                    writer.Write(")");
                    break;
                case OperatorType.And:
                case OperatorType.AndNot:
                case OperatorType.Or:
                case OperatorType.OrNot:
                    writer.Write("(");
                    Left.ToString(query, writer);
                    switch (Type)
                    {
                        case OperatorType.And:
                            writer.Write(" AND ");
                            break;
                        case OperatorType.AndNot:
                            writer.Write(" AND NOT ");
                            break;
                        case OperatorType.Or:
                            writer.Write(" OR ");
                            break;
                        case OperatorType.OrNot:
                            writer.Write(" OR NOT ");
                            break;
                    }
                    Right.ToString(query, writer);
                    writer.Write(")");
                    break;
                case OperatorType.Method:
                    writer.Write(Extract(Field));
                    writer.Write("(");

                    for (int i = 0; i < Arguments.Count; i++)
                    {
                        var arg = Arguments[i];
                        if (i != 0)
                            writer.Write(", ");
                        if (arg is QueryExpression qe)
                        {
                            qe.ToString(query, writer);
                        }
                        else if (arg is FieldToken field)
                        {
                            writer.Write(Extract(field));
                        }
                        else
                        {
                            var val = (ValueToken)arg;
                            writer.Write(Extract(val));
                        }
                    }
                    writer.Write(")");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void ToJsonAst(string query, JsonWriter writer)
        {
            if (Type == OperatorType.Field)
            {
                WriteValue(writer, Field);
                return;
            }
            writer.WriteStartObject();
            writer.WritePropertyName("Type");
            writer.WriteValue(Type.ToString());
            switch (Type)
            {
                case OperatorType.True:
                    break;
                case OperatorType.Equal:
                case OperatorType.NotEqual:
                case OperatorType.LessThan:
                case OperatorType.GreaterThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:

                    writer.WritePropertyName("Field");

                    if (Field != null)
                        WriteValue(writer, Field);
                    else
                        writer.WriteValue((string)null);

                    writer.WritePropertyName("Value");
                    switch (Value.Type)
                    {
                        case ValueTokenType.Null:
                            writer.WriteValue((string)null);
                            break;
                        case ValueTokenType.False:
                            writer.WriteValue(false);
                            break;
                        case ValueTokenType.True:
                            writer.WriteValue(true);
                            break;
                        default:
                            WriteValue(writer, Value, Value.Type == ValueTokenType.String);
                            break;
                    }

                    break;
                case OperatorType.Between:
                    writer.WritePropertyName("Field");
                    WriteValue( writer, Field);
                    writer.WritePropertyName("Min");
                    WriteValue( writer, First,
                        First.Type == ValueTokenType.Double || First.Type == ValueTokenType.Long);
                    writer.WritePropertyName("Max");
                    WriteValue( writer, Second,
                        Second.Type == ValueTokenType.Double || Second.Type == ValueTokenType.Long);
                    break;
                case OperatorType.In:
                case OperatorType.AllIn:
                    writer.WritePropertyName("Field");
                    WriteValue(writer, Field);
                    writer.WritePropertyName("Values");
                    writer.WriteStartArray();
                    foreach (var value in Values)
                    {
                        WriteValue(writer, value,
                            value.Type == ValueTokenType.Double || value.Type == ValueTokenType.Long);
                    }
                    writer.WriteEndArray();
                    break;
                case OperatorType.And:
                case OperatorType.AndNot:
                case OperatorType.Or:
                case OperatorType.OrNot:
                    writer.WritePropertyName("Left");
                    Left.ToJsonAst(query, writer);
                    writer.WritePropertyName("Right");
                    Right.ToJsonAst(query, writer);
                    break;
                case OperatorType.Method:
                    writer.WritePropertyName("Method");
                    WriteValue(writer, Field);
                    writer.WritePropertyName("Arguments");
                    writer.WriteStartArray();
                    foreach (var arg in Arguments)
                    {
                        if (arg is QueryExpression qe)
                        {
                            qe.ToJsonAst(query, writer);
                        }
                        else if (arg is FieldToken field)
                        {
                            writer.WriteStartObject();
                            writer.WritePropertyName("Field");
                            WriteValue(writer, field);
                            writer.WriteEndObject();
                        }
                        else
                        {
                            var val = (ValueToken)arg;
                            WriteValue(writer, val,
                                val.Type == ValueTokenType.Double || val.Type == ValueTokenType.Long);
                        }
                    }
                    writer.WriteEndArray();
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            writer.WriteEndObject();
        }

        private static void ThrowInvalidType(OperatorType type)
        {
            throw new ArgumentOutOfRangeException(type.ToString());
        }
    }
}
