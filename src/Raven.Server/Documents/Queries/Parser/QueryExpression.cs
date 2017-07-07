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

        [ThreadStatic] private static StringBuilder _tempBuffer;

        internal static string Extract(string q, ValueToken val)
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
            return Extract(q, val.TokenStart, val.TokenLength, val.EscapeChars);
        }

        internal static string Extract(string q, FieldToken field)
        {
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

        public void ToString(string query, TextWriter writer)
        {
            switch (Type)
            {
                case OperatorType.Field:
                    writer.Write(Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars));
                    writer.Write(" ");
                    break;
                case OperatorType.Equal:
                case OperatorType.LessThen:
                case OperatorType.GreaterThen:
                case OperatorType.LessThenEqual:
                case OperatorType.GreaterThenEqual:
                    writer.Write(Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars));
                    switch (Type)
                    {
                        case OperatorType.Equal:
                            writer.Write(" = ");
                            break;
                        case OperatorType.LessThen:
                            writer.Write(" < ");
                            break;
                        case OperatorType.GreaterThen:
                            writer.Write(" > ");
                            break;
                        case OperatorType.LessThenEqual:
                            writer.Write(" <= ");
                            break;
                        case OperatorType.GreaterThenEqual:
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
                    writer.Write(Extract(query, Field.TokenStart, Field.TokenLength, Field.EscapeChars));
                    writer.Write(" IN (");
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
                            var val = (ValueToken) arg;
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
                case OperatorType.Equal:
                case OperatorType.LessThen:
                case OperatorType.GreaterThen:
                case OperatorType.LessThenEqual:
                case OperatorType.GreaterThenEqual:
                    writer.WritePropertyName("Field");
                    WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
                    writer.WritePropertyName("Value");
                    switch (Value.Type)
                    {
                        case ValueTokenType.Null:
                            writer.WriteValue((string) null);
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
                            var val = (ValueToken) arg;
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