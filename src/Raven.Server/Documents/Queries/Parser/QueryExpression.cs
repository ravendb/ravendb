using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;

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

        internal static string Extract(string q, ValueToken val, bool stripQuotes = false)
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
                return Extract(q, val.TokenStart + 1, val.TokenLength - 2, val.EscapeChars);

            return Extract(q, val.TokenStart, val.TokenLength, val.EscapeChars);
        }

        internal static string Extract(string q, FieldToken field)
        {
            if (field.IsQuoted)
                return Extract(q, field.TokenStart + 1, field.TokenLength - 2, field.EscapeChars);
            return Extract(q, field.TokenStart, field.TokenLength, field.EscapeChars);
        }

        internal static string Extract(string q, int tokenStart, int tokenLength, int escapeChars)
        {
            if (escapeChars == 0)
            {
                return q.Substring(tokenStart, tokenLength);
            }
            var tmp = _tempBuffer ?? (_tempBuffer = new StringBuilder());
            tmp.Capacity = Math.Max(tmp.Capacity, tokenLength);
            var qouteChar = q[tokenStart];
            for (int i = tokenStart; i < tokenLength; i++)
            {
                if (q[i] != qouteChar)
                    tmp.Append(q[i]);
            }
            return tmp.ToString();
        }

        internal static void WriteValue(string q, JsonWriter writer, int tokenStart, int tokenLength, int escapeChars,
            bool raw = false)
        {
            if (escapeChars == 0)
            {
                if (tokenLength != 0)
                {
                    if (q[tokenStart] == '"' || q[tokenStart] == '\'')
                    {
                        // skip quotes
                        writer.WriteValue(q.Substring(tokenStart + 1, tokenLength - 1));
                        return;
                    }
                }
                if (raw)
                    writer.WriteRawValue(q.Substring(tokenStart, tokenLength));
                else
                    writer.WriteValue(q.Substring(tokenStart, tokenLength));
                return;
            }
            var tmp = _tempBuffer ?? (_tempBuffer = new StringBuilder());
            tmp.Capacity = Math.Max(tmp.Capacity, tokenLength);
            var qouteChar = q[tokenStart];
            for (int i = tokenStart + 1; i < tokenLength - 1; i++)
            {
                if (q[i] != qouteChar)
                    tmp.Append(q[i]);
            }
            writer.WriteValue(tmp.ToString());
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
                    writer.Write(Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars));
                    break;
                case OperatorType.Equal:
                case OperatorType.NotEqual:
                case OperatorType.LessThan:
                case OperatorType.GreaterThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:
                    var fieldName = Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
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
                    var method = Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
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
                            writer.Write(Extract(query, field.TokenStart, field.TokenLength, field.EscapeChars));
                        }
                        else
                        {
                            var val = (ValueToken)arg;
                            writer.Write(Extract(query, val));
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
            writer.Write(Extract(query, value));
        }

        public void ToString(string query, TextWriter writer)
        {
            switch (Type)
            {
                case OperatorType.Field:
                    writer.Write(Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars));
                    writer.Write(" ");
                    break;
                case OperatorType.Equal:
                case OperatorType.NotEqual:
                case OperatorType.LessThan:
                case OperatorType.GreaterThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThanEqual:
                    writer.Write(Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars));
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
                    writer.Write(Extract(query, Value));
                    break;
                case OperatorType.Between:
                    writer.Write(Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars));
                    writer.Write(" BETWEEN ");
                    writer.Write(Extract(query, First.TokenStart, First.TokenLength, First.EscapeChars));
                    writer.Write(" AND ");
                    writer.Write(Extract(query, Second.TokenStart, Second.TokenLength, Second.EscapeChars));
                    break;
                case OperatorType.In:
                case OperatorType.AllIn:
                    writer.Write(Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars));
                    writer.Write(Type != OperatorType.AllIn ? " IN (" : " ALL IN (");
                    for (var i = 0; i < Values.Count; i++)
                    {
                        var value = Values[i];
                        if (i != 0)
                            writer.Write(", ");
                        writer.Write(Extract(query, value));
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
                    writer.Write(Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars));
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
                            writer.Write(Extract(query, field.TokenStart, field.TokenLength, field.EscapeChars));
                        }
                        else
                        {
                            var val = (ValueToken)arg;
                            writer.Write(Extract(query, val));
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
                WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
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
                        WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
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
                            WriteValue(query, writer, Value.TokenStart, Value.TokenLength, Value.EscapeChars,
                                Value.Type == ValueTokenType.String);
                            break;
                    }

                    break;
                case OperatorType.Between:
                    writer.WritePropertyName("Field");
                    WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
                    writer.WritePropertyName("Min");
                    WriteValue(query, writer, First.TokenStart, First.TokenLength, First.EscapeChars,
                        First.Type == ValueTokenType.Double || First.Type == ValueTokenType.Long);
                    writer.WritePropertyName("Max");
                    WriteValue(query, writer, Second.TokenStart, Second.TokenLength, Second.EscapeChars,
                        Second.Type == ValueTokenType.Double || Second.Type == ValueTokenType.Long);
                    break;
                case OperatorType.In:
                case OperatorType.AllIn:
                    writer.WritePropertyName("Field");
                    WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
                    writer.WritePropertyName("Values");
                    writer.WriteStartArray();
                    foreach (var value in Values)
                    {
                        WriteValue(query, writer, value.TokenStart, value.TokenLength, value.EscapeChars,
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
                    WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
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
                            WriteValue(query, writer, field.TokenStart, field.TokenLength, field.EscapeChars);
                            writer.WriteEndObject();
                        }
                        else
                        {
                            var val = (ValueToken)arg;
                            WriteValue(query, writer, val.TokenStart, val.TokenLength, val.EscapeChars,
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
