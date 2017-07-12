using System;
using System.Text;
using Lucene.Net.Analysis;
using Lucene.Net.QueryParsers;
using Lucene.Net.Search;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries.Parse;
using Raven.Server.Utils;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Documents.Queries
{
    public static class QueryBuilder
    {
        private static TermLuceneASTNode WildCardTerm = new TermLuceneASTNode() { Term = "*", Type = TermLuceneASTNode.TermType.WildCardTerm };
        private static TermLuceneASTNode NullTerm = new TermLuceneASTNode() { Term = "NULL", Type = TermLuceneASTNode.TermType.Null };

        public static bool UseLuceneASTParser { get; set; } = true;

        public static Lucene.Net.Search.Query BuildQuery(string query, Analyzer analyzer)
        {
            throw new NotSupportedException("TODO arek - remove me");
        }

        public static Lucene.Net.Search.Query BuildQuery(Parser.Query query, WhereFields whereFields, Analyzer analyzer)
        {
            using (CultureHelper.EnsureInvariantCulture())
            {
                var node = ToLuceneNode(query.QueryText, query.Where, whereFields);

                var luceneQuery = node.ToQuery(new LuceneASTQueryConfiguration
                {
                    Analayzer = analyzer,
                    DefaultOperator = QueryOperator.And,
                    FieldName = new FieldName(string.Empty)
                });

                return luceneQuery;
            }
        }

        private static LuceneASTNodeBase ToLuceneNode(string query, QueryExpression expression, WhereFields whereFields)
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
                case OperatorType.GreaterThen:
                case OperatorType.LessThen:
                case OperatorType.LessThenEqual:
                case OperatorType.GreaterThenEqual:
                    var fieldName = QueryExpression.Extract(query, expression.Field);
                    var (value, valueType) = whereFields[fieldName];

                    var luceneFieldName = IndexField.ReplaceInvalidCharactersInFieldName(fieldName);

                    FieldName.FieldType fieldType = FieldName.FieldType.String;

                    switch (valueType)
                    {
                        case ValueTokenType.String:
                            fieldType = FieldName.FieldType.String;
                            break;
                        case ValueTokenType.Double:
                            luceneFieldName += Client.Constants.Documents.Indexing.Fields.RangeFieldSuffixDouble;
                            fieldType = FieldName.FieldType.Double;
                            break;
                        case ValueTokenType.Long:
                            luceneFieldName += Client.Constants.Documents.Indexing.Fields.RangeFieldSuffixLong;
                            fieldType = FieldName.FieldType.Long;
                            break;
                    }

                    if (expression.Type == OperatorType.Equal && fieldType == FieldName.FieldType.String)
                    {
                        return new FieldLuceneASTNode()
                        {
                            FieldName = new FieldName(luceneFieldName, fieldType),
                            Node = CreateTermNode(value, valueType)
                        };
                    }

                    RangeLuceneASTNode rangeNode = new RangeLuceneASTNode()
                    {
                        InclusiveMin = false,
                        InclusiveMax = false,
                        RangeMin = WildCardTerm,
                        RangeMax = NullTerm
                    };

                    switch (expression.Type)
                    {
                        case OperatorType.Equal:
                            rangeNode.InclusiveMin = true;
                            rangeNode.InclusiveMax = true;
                            var node = CreateTermNode(value, valueType);
                            rangeNode.RangeMin = node;
                            rangeNode.RangeMax = node;
                            break;
                        case OperatorType.LessThen:
                            rangeNode.RangeMax = CreateTermNode(value, valueType);
                            break;
                        case OperatorType.GreaterThen:
                            rangeNode.RangeMin = CreateTermNode(value, valueType);
                            break;
                        case OperatorType.LessThenEqual:
                            rangeNode.InclusiveMax = true;
                            rangeNode.RangeMax = CreateTermNode(value, valueType);
                            break;
                        case OperatorType.GreaterThenEqual:
                            rangeNode.InclusiveMin = true;
                            rangeNode.RangeMin = CreateTermNode(value, valueType);
                            break;
                        default:
                            break;
                    }

                    return new FieldLuceneASTNode()
                    {
                        FieldName = new FieldName(luceneFieldName, fieldType),
                        Node = rangeNode
                    };
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
                    return new OperatorLuceneASTNode(ToLuceneNode(query, expression.Left, whereFields), ToLuceneNode(query, expression.Right, whereFields), OperatorLuceneASTNode.Operator.AND,
                        true);
                case OperatorType.Or:
                    return new OperatorLuceneASTNode(ToLuceneNode(query, expression.Left, whereFields), ToLuceneNode(query, expression.Right, whereFields), OperatorLuceneASTNode.Operator.OR,
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

        private static TermLuceneASTNode CreateTermNode(string value, ValueTokenType valueType)
        {
            switch (valueType)
            {
                case ValueTokenType.Null:
                    return new TermLuceneASTNode()
                    {
                        Type = TermLuceneASTNode.TermType.Null
                    };
                case ValueTokenType.False:
                case ValueTokenType.True:
                    throw new NotImplementedException("expression.Value.Type:" + valueType);
                default:
                    TermLuceneASTNode.TermType type;
                   
                    switch (valueType)
                    {
                        case ValueTokenType.String:
                            type = TermLuceneASTNode.TermType.Quoted;
                            break;
                        case ValueTokenType.Long:
                            type = TermLuceneASTNode.TermType.Long;
                            break;
                        case ValueTokenType.Double:
                            type = TermLuceneASTNode.TermType.Double;
                            break;
                        default:
                            throw new NotImplementedException("Unhandled value type: " + valueType);
                            //type = TermLuceneASTNode.TermType.Quoted;
                            //break;
                    }

                    return new TermLuceneASTNode()
                    {
                        Term = value,
                        Type = type,
                    };
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