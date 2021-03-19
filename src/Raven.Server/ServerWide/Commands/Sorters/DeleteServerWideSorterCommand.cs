using Raven.Server.Documents.Indexes.Sorting;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.ServerWide.Commands.Sorters
{
    public class DeleteServerWideSorterCommand : DeleteValueCommand
    {
        public DeleteServerWideSorterCommand()
        {
            // for deserialization
        }

        public DeleteServerWideSorterCommand(string name, string uniqueRequestId)
            : base(uniqueRequestId)
        {
            Name = PutServerWideSorterCommand.GetName(name);
        }

        public override void DeleteValue(ClusterOperationContext context)
        {
            context.Transaction.InnerTransaction.LowLevelTransaction.BeforeCommitFinalization += _ => SorterCompilationCache.Instance.RemoveServerWideItem(PutServerWideSorterCommand.ExtractName(Name));
        }
    }
}
