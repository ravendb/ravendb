using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public class Query
    {
        public bool IsDistinct;
        public QueryExpression Where;
        public (FieldToken From, FieldToken Alias, QueryExpression Filter, bool Index) From;
        public List<(QueryExpression Expression, FieldToken Alias)> Select;
        public List<(QueryExpression Expression, FieldToken Alias)> With;
        public List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)> OrderBy;
        public List<FieldToken> GroupBy;

        public Dictionary<StringSegment, string> DeclaredFunctions;

        public string QueryText;
        public ValueToken SelectFunctionBody { get; set; }

        public string ToJsonAst()
        {
            var output = new StringWriter();
            ToJsonAst(new JsonTextWriter(output));
            return output.GetStringBuilder().ToString();
        }

        public override string ToString()
        {
            var writer = new StringWriter();
            if (DeclaredFunctions != null)
            {
                foreach (var function in DeclaredFunctions)
                {
                    writer.Write("DECLARE ");
                    writer.WriteLine(function.Value);
                }
            }
            if (Select != null)
            {
                WriteSelectOrWith("SELECT", writer, Select, IsDistinct);
            }
           
            writer.Write(" FROM ");
            if (From.Index)
            {
                writer.Write("INDEX ");
                writer.Write(QueryExpression.Extract(QueryText, From.From.TokenStart, From.From.TokenLength, From.From.EscapeChars));
            }
            else if (From.Filter != null)
            {
                writer.Write("(");
                writer.Write(QueryExpression.Extract(QueryText, From.From.TokenStart, From.From.TokenLength, From.From.EscapeChars));
                writer.Write(", ");
                From.Filter.ToString(QueryText, writer);
                writer.Write(")");
            }
            else
            {
                writer.Write(QueryExpression.Extract(QueryText, From.From.TokenStart, From.From.TokenLength, From.From.EscapeChars));
            }

            if (From.Alias != null)
            {
                writer.Write(" AS ");
                writer.Write(QueryExpression.Extract(QueryText, From.Alias.TokenStart, From.Alias.TokenLength, From.Alias.EscapeChars));
            }

            writer.WriteLine();
            if (With != null)
            {
                WriteSelectOrWith("WITH", writer, With, isDistinct: false);
            }
            if (GroupBy != null)
            {
                writer.Write("GROUP BY ");
                for (var index = 0; index < GroupBy.Count; index++)
                {
                    if (index != 0)
                        writer.Write(", ");
                    var field = GroupBy[index];
                    writer.Write(QueryExpression.Extract(QueryText, field.TokenStart, field.TokenLength, field.EscapeChars));
                }
                writer.WriteLine();
            }
            if (Where != null)
            {
                writer.Write("WHERE ");
                Where.ToString(QueryText, writer);
                writer.WriteLine();
            }
            if (OrderBy != null)
            {
                writer.Write("ORDER BY ");
                for (var index = 0; index < OrderBy.Count; index++)
                {
                    if (index != 0)
                        writer.Write(", ");
                    OrderBy[index].Expression.ToString(QueryText, writer);
                    switch (OrderBy[index].FieldType)
                    {
                        case OrderByFieldType.String:
                            writer.Write(" AS string");
                            break;
                        case OrderByFieldType.Long:
                            writer.Write(" AS long");
                            break;
                        case OrderByFieldType.Double:
                            writer.Write(" AS double");
                            break;
                        case OrderByFieldType.AlphaNumeric:
                            writer.Write(" AS alphanumeric");
                            break;
                    }
                    
                    if (OrderBy[index].Ascending == false)
                        writer.Write(" DESC");
                }
                writer.WriteLine();
            }
            return writer.GetStringBuilder().ToString();
        }

        private void WriteSelectOrWith(string clause, StringWriter writer, List<(QueryExpression Expression, FieldToken Alias)> clauseItems, bool isDistinct)
        {
            writer.Write(clause);
            writer.Write(" ");
            if (isDistinct)
                writer.Write("DISTINCT ");

            for (var index = 0; index < clauseItems.Count; index++)
            {
                if (index != 0)
                    writer.Write(", ");
                var item = clauseItems[index];
                item.Expression.ToString(QueryText, writer);
                if (item.Alias != null)
                {
                    writer.Write("AS ");
                    writer.Write(QueryExpression.Extract(QueryText, item.Alias.TokenStart, item.Alias.TokenLength,
                        item.Alias.EscapeChars));
                }
            }
            writer.WriteLine();
        }

        public void ToJsonAst(JsonWriter writer)
        {
            writer.WriteStartObject();
            if (DeclaredFunctions != null)
            {
                writer.WritePropertyName("Declare");
                writer.WriteStartObject();
                foreach (var declaredFunction in DeclaredFunctions)
                {
                    writer.WritePropertyName(declaredFunction.Key);
                    writer.WriteValue(declaredFunction.Value);
                }
                writer.WriteEndObject();
            }
            if (Select != null)
            {
                WriteSelectOrWith(writer, Select, "Select",QueryText);
            }
            if (With != null)
            {
                WriteSelectOrWith(writer, With, "With",QueryText);
            }
            writer.WritePropertyName("From");
            writer.WriteStartObject();
            writer.WritePropertyName("Index");
            writer.WriteValue(From.Index);
            writer.WritePropertyName("Source");
            QueryExpression.WriteValue(QueryText, writer, From.From.TokenStart, From.From.TokenLength,
                      From.From.EscapeChars);
            if (From.Filter != null)
            {
                writer.WritePropertyName("Filter");
                From.Filter.ToJsonAst(QueryText, writer);
            }
            if (From.Alias != null)
            {
                writer.WritePropertyName("Alias");
                QueryExpression.WriteValue(QueryText, writer, From.Alias.TokenStart, From.Alias.TokenLength,
                    From.Alias.EscapeChars);
            }
            writer.WriteEndObject();

            if (GroupBy != null)
            {
                writer.WritePropertyName("GroupBy");
                writer.WriteStartArray();
                foreach (var field in GroupBy)
                {
                    QueryExpression.WriteValue(QueryText, writer, field.TokenStart, field.TokenLength, field.EscapeChars);
                }
                writer.WriteEndArray();
            }

            if (Where != null)
            {
                writer.WritePropertyName("Where");
                Where.ToJsonAst(QueryText, writer);
            }
            if (OrderBy != null)
            {
                writer.WritePropertyName("OrderBy");
                writer.WriteStartArray();
                foreach (var field in OrderBy)
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Field");
                    field.Expression.ToJsonAst(QueryText, writer);

                    if (field.FieldType != OrderByFieldType.Implicit)
                    {
                        writer.WritePropertyName("FieldType");
                        writer.WriteValue(field.FieldType.ToString());
                    }

                    writer.WritePropertyName("Ascending");
                    writer.WriteValue(field.Ascending);
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
            }
            writer.WriteEndObject();
        }

        public static void WriteSelectOrWith(JsonWriter writer, List<(QueryExpression Expression, FieldToken Alias)> clauseItems, string clause, string queryText)
        {
            writer.WritePropertyName(clause);
            writer.WriteStartArray();

            foreach (var field in clauseItems)
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Expression");
                field.Expression.ToJsonAst(queryText, writer);
                if (field.Alias != null)
                {
                    writer.WritePropertyName("Alias");
                    QueryExpression.WriteValue(queryText, writer, field.Alias.TokenStart, field.Alias.TokenLength,
                        field.Alias.EscapeChars);
                }
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }

        public bool TryAddFunction(StringSegment name, string func)
        {
            if (DeclaredFunctions == null)
                DeclaredFunctions = new Dictionary<StringSegment, string>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

            return DeclaredFunctions.TryAdd(name, func);
        }
    }
}
