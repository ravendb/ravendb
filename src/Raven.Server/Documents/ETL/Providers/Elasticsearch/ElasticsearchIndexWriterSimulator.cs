using System.Collections.Generic;
using System.Text;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public class ElasticSearchIndexWriterSimulator
    {
        public IEnumerable<string> SimulateExecuteCommandText(ElasticSearchIndexWithRecords records, DocumentsOperationContext context)
        {
            var result = new List<string>();

            if (records.InsertOnlyMode == false)
            {
                // first, delete all the rows that might already exist there

                result.Add(GenerateDeleteItemsCommandText(records.IndexName.ToLower(), records.DocumentIdProperty,
                    records.Deletes));
            }

            result.AddRange(GenerateInsertItemsCommandText(records.IndexName.ToLower(), records, context));

            return result;
        }

        private string GenerateDeleteItemsCommandText(string indexName, string idField, List<ElasticSearchItem> elasticSearchItems)
        {
            var idsToDelete = new List<string>();

            foreach (var item in elasticSearchItems)
            {
                idsToDelete.Add(ElasticSearchEtl.LowerCaseDocumentIdProperty(item.DocumentId));
            }

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var result = new DynamicJsonValue()
                {
                    ["query"] = new DynamicJsonValue()
                    {
                        ["terms"] = new DynamicJsonValue()
                        {
                            [idField] = new DynamicJsonArray(idsToDelete)
                        }
                    }
                };

                var resultJson = context.ReadObject(result, "").ToString();

                var sb = new StringBuilder("POST ")
                    .Append(indexName)
                    .AppendLine("/_delete_by_query?refresh=true")
                    .AppendLine(resultJson);

                return sb.ToString();
            }
        }

        private IEnumerable<string> GenerateInsertItemsCommandText(string indexName, ElasticSearchIndexWithRecords index, DocumentsOperationContext context)
        {
            var result = new List<string>();

            if (index.Inserts.Count > 0)
            {
                var sb = new StringBuilder("POST ")
                    .Append(indexName)
                    .AppendLine("/_bulk?refresh=wait_for");

                foreach (var item in index.Inserts)
                {
                    using (var json = ElasticSearchEtl.EnsureLowerCasedIndexIdProperty(context, item.TransformationResult, index))
                    {
                        sb.AppendLine(ElasticSearchEtl.IndexBulkAction);
                        sb.AppendLine(json.ToString());
                    }
                }

                result.Add(sb.ToString());
            }

            return result;
        }
    }
}
