using Raven.Server.Documents.Indexes.Analysis;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands.Analyzers
{
    public class DeleteServerWideAnalyzerCommand : DeleteValueCommand
    {
        public DeleteServerWideAnalyzerCommand()
        {
            // for deserialization
        }

        public DeleteServerWideAnalyzerCommand(string name, string uniqueRequestId)
            : base(uniqueRequestId)
        {
            Name = PutServerWideAnalyzerCommand.GetName(name);
        }

        public override void DeleteValue(ClusterOperationContext context)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.BeforeCommitFinalization += _ => AnalyzerCompilationCache.Instance.RemoveServerWideItem(PutServerWideAnalyzerCommand.ExtractName(Name));
        }
    }
}
