using System.Collections.Generic;
using Raven.Client.Documents.Operations.ConnectionStrings;

namespace Raven.Client.Documents.Operations.ETL.AI;

public sealed class AiConnectionString : ConnectionString
{
    public override ConnectionStringType Type => ConnectionStringType.Ai;
    protected override void ValidateImpl(ref List<string> errors)
    {
        
    }
}
