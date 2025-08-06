using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Test
{
    public abstract class TestEtlScript<TConfiguration, TConnectionString> 
        where TConfiguration : EtlConfiguration<TConnectionString> 
        where TConnectionString : ConnectionString
    {
        public string DocumentId;

        public bool IsDelete;

        public TConfiguration Configuration;


        public DynamicJsonValue ToJson() => new DynamicJsonValue()
        {
            [nameof(DocumentId)] = DocumentId,
            [nameof(IsDelete)] = IsDelete,
            [nameof(Configuration)] = Configuration.ToJson()
        };
    }
}
