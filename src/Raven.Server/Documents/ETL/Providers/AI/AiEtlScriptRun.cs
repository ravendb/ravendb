using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.AI;

public class AiEtlScriptRun
{
    public Dictionary<string, Dictionary<string, List<string>>> CurrentRun { get; set; }

    public AiEtlScriptRun()
    {
        CurrentRun = new Dictionary<string, Dictionary<string, List<string>>>();
    }

    public void Add(string documentId, Dictionary<string, List<string>> textValues)
    {
        CurrentRun.Add(documentId, textValues);
    }
}
