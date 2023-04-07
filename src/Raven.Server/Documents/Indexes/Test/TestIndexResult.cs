using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Test;

public class TestIndexResult
{
    public List<BlittableJsonReaderObject> IndexEntries;
    public List<Document> QueryResults;
    public List<BlittableJsonReaderObject> MapResults;
    public List<BlittableJsonReaderObject> ReduceResults;
    public bool HasDynamicFields;
    
    public async Task WriteTestIndexResult(Stream responseBodyStream, DocumentsOperationContext context)
    {
        await using (var writer = new AsyncBlittableJsonTextWriter(context, responseBodyStream))
        {
            writer.WriteStartObject();

            writer.WriteArray(context, nameof(IndexEntries), IndexEntries, (w, c, indexEntry) =>
            {
                w.WriteObject(indexEntry);
            });
                        
            writer.WriteComma();
                        
            writer.WriteArray(context, nameof(QueryResults), QueryResults, (w, c, queryResult) =>
            {
                w.WriteObject(queryResult.Data);
            });
                        
            writer.WriteComma();
            
            writer.WriteArray(context, nameof(MapResults), MapResults, (w, c, mapResult) =>
            {
                w.WriteObject(mapResult);
            });

            writer.WriteComma();
            
            writer.WriteArray(context, nameof(ReduceResults), ReduceResults, (w, c, indexingFunctionResult) =>
            {
                w.WriteObject(indexingFunctionResult);
            });
            
            writer.WriteComma();
            
            writer.WritePropertyName(nameof(HasDynamicFields));
            writer.WriteBool(HasDynamicFields);

            writer.WriteEndObject();
        }
    }
}
