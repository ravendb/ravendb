using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Lucene.Net.Analysis;
using Lucene.Net.Index;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Raven.Server.Documents.Queries.Parse;
using Raven.Server.Utils;
using Version = Lucene.Net.Util.Version;
using Raven.Server.Documents.Queries.Parser;

namespace Raven.Server.Documents.Queries
{
    public static class QueryBuilder
    {
        public static bool UseLuceneASTParser { get; set; } = true;

        public static Lucene.Net.Search.Query BuildQuery(string query, Analyzer analyzer)
        {
            throw new NotSupportedException("TODO arek - remove me");
        }

        public static Lucene.Net.Search.Query BuildQuery(Parser.Query query, Analyzer analyzer)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var node = ToLuceneNode(query.QueryText, query.Where);

                return node.ToQuery(new LuceneASTQueryConfiguration
                {
                    Analayzer = analyzer,
                    DefaultOperator = QueryOperator.And,
                    FieldName = new FieldName(string.Empty)
                });
            }
        }

        private static LuceneASTNodeBase ToLuceneNode(string query, QueryExpression expression)
        {
            if (expression == null)
                return new AllDocumentsLuceneASTNode();

            if (expression.Type == OperatorType.Field)
            {
                throw new NotImplementedException("OperatorType.Field");

//                return new FieldLuceneASTNode()
//                {
//                    FieldName = new FieldName(QueryExpression.Extract(query, expression.Field)),
//                    Node = new TermLuceneASTNode()
//                    {
//                        Term = QueryExpression.Extract(query, expression.Value ?? expression.First),
//                    }
//                };
            }

            switch (expression.Type)
            {
                case OperatorType.Equal:
                    var fieldNode = new FieldLuceneASTNode()
                    {
                        FieldName = new FieldName(QueryExpression.Extract(query, expression.Field)),
                    };

                    switch (expression.Value.Type)
                    {
                        case ValueTokenType.Null:
                            fieldNode.Node = new TermLuceneASTNode()
                            {
                                Type = TermLuceneASTNode.TermType.Null
                            };
                            break;
                        case ValueTokenType.False:
                        case ValueTokenType.True:
                            throw new NotImplementedException("expression.Value.Type:" + expression.Type);
                        default:
                            var value = expression.Value ?? expression.First;
                            string term;
                            switch (value.Type)
                            {
                                case ValueTokenType.String:
                                    term = QueryExpression.Extract(query, value.TokenStart + 1, value.TokenLength - 1, value.EscapeChars);
                                    break;
                                default:
                                    term = QueryExpression.Extract(query, value);
                                    break;
                            }

                            fieldNode.Node = new TermLuceneASTNode()
                            {
                                Term = term,
                            };
                            break;
                    }

                    return fieldNode;
                case OperatorType.GreaterThen:

                case OperatorType.LessThen:
                case OperatorType.LessThenEqual:
                case OperatorType.GreaterThenEqual:

                    throw new NotImplementedException("Type:" + expression.Type);


                //    writer.WritePropertyName("Field");
                //    WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
                //    writer.WritePropertyName("Value");
                //    switch (Value.Type)
                //    {
                //        case ValueTokenType.Null:
                //            writer.WriteValue((string)null);
                //            break;
                //        case ValueTokenType.False:
                //            writer.WriteValue(false);
                //            break;
                //        case ValueTokenType.True:
                //            writer.WriteValue(true);
                //            break;
                //        default:
                //            WriteValue(query, writer, Value.TokenStart, Value.TokenLength, Value.EscapeChars,
                //                Value.Type == ValueTokenType.String);
                //            break;
                //    }

                //    break;
                //case OperatorType.Between:
                //    writer.WritePropertyName("Field");
                //    WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
                //    writer.WritePropertyName("Min");
                //    WriteValue(query, writer, First.TokenStart, First.TokenLength, First.EscapeChars,
                //        First.Type == ValueTokenType.Double || First.Type == ValueTokenType.Long);
                //    writer.WritePropertyName("Max");
                //    WriteValue(query, writer, Second.TokenStart, Second.TokenLength, Second.EscapeChars,
                //        Second.Type == ValueTokenType.Double || Second.Type == ValueTokenType.Long);
                //    break;
                //case OperatorType.In:
                //    writer.WritePropertyName("Field");
                //    WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
                //    writer.WritePropertyName("Values");
                //    writer.WriteStartArray();
                //    foreach (var value in Values)
                //    {
                //        WriteValue(query, writer, value.TokenStart, value.TokenLength, value.EscapeChars,
                //            value.Type == ValueTokenType.Double || value.Type == ValueTokenType.Long);
                //    }
                //    writer.WriteEndArray();
                //break;
                case OperatorType.And:
                    return new OperatorLuceneASTNode(ToLuceneNode(query, expression.Left), ToLuceneNode(query, expression.Right), OperatorLuceneASTNode.Operator.AND,
                        true);
                case OperatorType.Or:
                    return new OperatorLuceneASTNode(ToLuceneNode(query, expression.Left), ToLuceneNode(query, expression.Right), OperatorLuceneASTNode.Operator.OR,
                        true);
                case OperatorType.AndNot:
                case OperatorType.OrNot:
                case OperatorType.Method:
                    throw new NotImplementedException("Type: " + expression.Type);
                //writer.WritePropertyName("Method");
                //WriteValue(query, writer, Field.TokenStart, Field.TokenLength, Field.EscapeChars);
                //writer.WritePropertyName("Arguments");
                //writer.WriteStartArray();
                //foreach (var arg in Arguments)
                //{
                //    if (arg is QueryExpression qe)
                //    {
                //        qe.ToJsonAst(query, writer);
                //    }
                //    else if (arg is FieldToken field)
                //    {
                //        writer.WriteStartObject();
                //        writer.WritePropertyName("Field");
                //        WriteValue(query, writer, field.TokenStart, field.TokenLength, field.EscapeChars);
                //        writer.WriteEndObject();
                //    }
                //    else
                //    {
                //        var val = (ValueToken)arg;
                //        WriteValue(query, writer, val.TokenStart, val.TokenLength, val.EscapeChars,
                //            val.Type == ValueTokenType.Double || val.Type == ValueTokenType.Long);
                //    }
                //}
                //writer.WriteEndArray();
                //break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static Lucene.Net.Search.Query Lucene(string query, QueryOperator defaultOperator, string defaultField, Analyzer analyzer)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                try
                {
                    var parser = new LuceneQueryParser();
                    parser.IsDefaultOperatorAnd = defaultOperator == QueryOperator.And;
                    parser.Parse(query);

                    var res = parser.LuceneAST.ToQuery(new LuceneASTQueryConfiguration
                    {
                        Analayzer = analyzer,
                        DefaultOperator = QueryOperator.And,
                        FieldName = new FieldName(defaultField ?? string.Empty)
                    });
                    // The parser already throws parse exception if there is a syntax error.
                    // We now return null in the case of a term query that has been fully analyzed, so we need to return a valid query.
                    return res ?? new BooleanQuery();
                }
                catch (ParseException pe)
                {
                    throw new ParseException("Could not parse: '" + query + "'", pe);
                }
            }
        }

        public static string Unescape(string term)
        {
            // method doesn't allocate a StringBuilder unless the string requires unescaping
            // also this copies chunks of the original string into the StringBuilder which
            // is far more efficient than copying character by character because StringBuilder
            // can access the underlying string data directly

            if (string.IsNullOrEmpty(term))
            {
                return term;
            }

            bool isPhrase = term.StartsWith("\"") && term.EndsWith("\"");
            int start = 0;
            int length = term.Length;
            StringBuilder buffer = null;
            char prev = '\0';
            for (int i = start; i < length; i++)
            {
                char ch = term[i];
                if (prev != '\\')
                {
                    prev = ch;
                    continue;
                }
                prev = '\0'; // reset
                switch (ch)
                {
                    case '*':
                    case '?':
                    case '+':
                    case '-':
                    case '&':
                    case '|':
                    case '!':
                    case '(':
                    case ')':
                    case '{':
                    case '}':
                    case '[':
                    case ']':
                    case '^':
                    case '"':
                    case '~':
                    case ':':
                    case '\\':
                    {
                        if (buffer == null)
                        {
                            // allocate builder with headroom
                            buffer = new StringBuilder(length * 2);
                        }
                        // append any leading substring
                        buffer.Append(term, start, i - start - 1);
                        buffer.Append(ch);
                        start = i + 1;
                        break;
                    }
                }
            }

            if (buffer == null)
            {
                if (isPhrase)
                    return term.Substring(1, term.Length - 2);
                // no changes required
                return term;
            }

            if (length > start)
            {
                // append any trailing substring
                buffer.Append(term, start, length - start);
            }

            return buffer.ToString();
        }
    }
}