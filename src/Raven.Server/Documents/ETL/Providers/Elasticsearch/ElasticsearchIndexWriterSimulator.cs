using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public class ElasticSearchIndexWriterSimulator
    {
        public IEnumerable<string> SimulateExecuteCommandText(ElasticSearchIndexWithRecords records)
        {
            var result = new List<string>();

            // first, delete all the rows that might already exist there
            result.Add(GenerateDeleteItemsCommandText(records.IndexName.ToLower(), records.IndexIdProperty,
                records.Deletes));

            result.AddRange(GenerateInsertItemsCommandText(records.IndexName.ToLower(), records.Inserts));

            return result;
        }

        private string GenerateDeleteItemsCommandText(string indexName, string idField, List<ElasticSearchItem> elasticItems)
        {
            StringBuilder deleteQuery = new StringBuilder();

            foreach (var t in elasticItems)
            {
                deleteQuery.Append($"{t.DocumentId},");
            }

            // arek will sent example
            var sb = new StringBuilder("POST ")
                .Append(indexName)
                .AppendLine("/_delete_by_query")
                .AppendLine("{")
                .AppendLine("    \"query\": {")
                .AppendLine("      \"match\": {")
                .Append("        \"")
                .Append(idField)
                .Append("\"")
                .Append(":")
                .Append("\"")
                .Append(deleteQuery)
                .AppendLine("\"")
                .AppendLine("      }")
                .AppendLine("    }")
                .AppendLine("}");

            return sb.ToString();
        }

        private IEnumerable<string> GenerateInsertItemsCommandText(string indexName, List<ElasticSearchItem> elasticsearchItems)
        {
            var result = new List<string>();

            foreach (var item in elasticsearchItems)
            {
                var sb = new StringBuilder("POST ")
                    .Append(indexName)
                    .AppendLine("/_doc")
                    .AppendLine("{")
                    .AppendLine(item.Property.RawValue.ToString().Replace("{", "").Replace("}", ""))
                    .AppendLine("}");

                result.Add(sb.ToString());
            }

            return result;
        }
    }
}
