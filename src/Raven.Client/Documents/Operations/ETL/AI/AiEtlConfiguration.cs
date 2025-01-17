using System.Collections.Generic;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class AiEtlConfiguration : EtlConfiguration<AiConnectionString>
{
    public List<string> FieldsToInclude { get; set; }
    
    public override string GetDestination()
    {
        return "something";
    }

    public override EtlType EtlType => EtlType.OpenAi;
    public override bool UsingEncryptedCommunicationChannel()
    {
        throw new System.NotImplementedException();
    }

    public override string GetDefaultTaskName()
    {
        throw new System.NotImplementedException();
    }
}
