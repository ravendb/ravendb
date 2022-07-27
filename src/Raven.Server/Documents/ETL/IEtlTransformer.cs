using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.ETL;

namespace Raven.Server.Documents.ETL;

public interface IEtlTransformer<TExtracted, TTransformed, TStatsScope> : IDisposable
{
    public IEnumerable<TTransformed> GetTransformedResults();
    public void Transform(TExtracted item, TStatsScope stats, EtlProcessState state);
    public void Initialize(bool debugMode);
    public List<string> GetDebugOutput();
}