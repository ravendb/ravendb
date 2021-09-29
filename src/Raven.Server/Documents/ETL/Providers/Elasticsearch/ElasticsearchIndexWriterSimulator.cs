using System;
using System.Collections.Generic;
using System.Text;
using NuGet.Protocol;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

        private string GenerateDeleteItemsCommandText(string indexName, string idField, List<ElasticSearchItem> elasticSearchItems)
        {
            StringBuilder deleteQuery = new StringBuilder();

            foreach (var item in elasticSearchItems)
            {
                deleteQuery.Append($"{item.DocumentId},");
            }

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var result = new DynamicJsonValue()
                {
                    ["query"] = new DynamicJsonValue()
                    {
                        ["match"] = new DynamicJsonValue()
                        {
                            [idField] = deleteQuery.ToString()
                        }
                    }
                };

                var resultJson = context.ReadObject(result, "").ToString();

                var sb = new StringBuilder("POST ")
                    .Append(indexName)
                    .AppendLine("/_delete_by_query")
                    .AppendLine(resultJson);

                return sb.ToString();
            }
        }

        private IEnumerable<string> GenerateInsertItemsCommandText(string indexName, List<ElasticSearchItem> elasticSearchItems)
        {
            var result = new List<string>();

            foreach (var item in elasticSearchItems)
            {
                var sb = new StringBuilder("POST ")
                    .Append(indexName)
                    .AppendLine("/_doc")
                    .AppendLine(item.Property.RawValue.ToString());

                result.Add(sb.ToString());
            }

            return result;
        }
    }
}
