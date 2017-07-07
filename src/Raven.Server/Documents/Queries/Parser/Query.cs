using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;

namespace Raven.Server.Documents.Queries.Parser
{
    public class Query
    {
        public QueryExpression Where;
        public (FieldToken From, FieldToken Alias, QueryExpression Filter, bool Index) From;
        public List<(QueryExpression Expression, FieldToken Alias)> Select;
        public List<(QueryExpression Expression, FieldToken Alias)> With;
        public List<(FieldToken Field, bool Ascending)> OrderBy;
        public List<FieldToken> GroupBy;
        public string QueryText;
        
        public override string ToString()
        {
            var writer = new StringWriter();
            if(Select != null)
            {
                WriteSelectOrWith("SELECT", writer, Select);
            }
            if(With != null)
            {
                WriteSelectOrWith("WITH", writer, With);
            }
            writer.Write("FROM ");
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
            if(Where != null)
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
                    var f = OrderBy[index];
                    writer.Write(QueryExpression.Extract(QueryText, f.Field.TokenStart, f.Field.TokenLength, f.Field.EscapeChars));
                    if(f.Ascending == false)
                        writer.Write(" DESC");
                }
                writer.WriteLine();
            }
            return writer.GetStringBuilder().ToString();
        }

        private void WriteSelectOrWith(string clause,StringWriter writer, List<(QueryExpression Expression, FieldToken Alias)> clauseItems)
        {
            writer.Write(clause);
            writer.Write(" ");
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
            if (Select != null)
            {
                WriteSelectOrWith(writer, Select, "Select");
            }
            if (With != null)
            {
                WriteSelectOrWith(writer, With, "With");
            }
            writer.WritePropertyName("From");
            writer.WriteStartObject();
            writer.WritePropertyName("Index");
            writer.WriteValue(From.Index);
            writer.WritePropertyName("Source");
            QueryExpression.WriteValue(QueryText, writer, From.From.TokenStart, From.From.TokenLength,
                      From.From.EscapeChars);
            if(From.Filter != null)
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
                    QueryExpression.WriteValue(QueryText, writer, field.Field.TokenStart, field.Field.TokenLength,
                        field.Field.EscapeChars);
                    writer.WritePropertyName("Ascending");
                    writer.WriteValue(field.Ascending);
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
            }
            writer.WriteEndObject();
        }

        private void WriteSelectOrWith(JsonWriter writer, List<(QueryExpression Expression, FieldToken Alias)> clauseItems, string clause)
        {
            writer.WritePropertyName(clause);
            writer.WriteStartArray();

            foreach (var field in clauseItems)
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Expression");
                field.Expression.ToJsonAst(QueryText, writer);
                if (field.Alias != null)
                {
                    writer.WritePropertyName("Alias");
                    QueryExpression.WriteValue(QueryText, writer, field.Alias.TokenStart, field.Alias.TokenLength,
                        field.Alias.EscapeChars);
                }
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
        }
    }
}