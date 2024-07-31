using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Revisions;

internal abstract class AbstractDeleteRevisionsMergedCommand<TResult> : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
{
    public TResult Result;

    protected readonly bool IncludeForceCreated;

    protected AbstractDeleteRevisionsMergedCommand(bool includeForceCreated)
    {
        IncludeForceCreated = includeForceCreated;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        Result = DeleteRevisions(context);
        return 1;
    }

    protected abstract TResult DeleteRevisions(DocumentsOperationContext context);

    protected bool SkipForceCreated(Document revision)
    {
        return IncludeForceCreated == false && revision.Flags.Contain(DocumentFlags.ForceCreated);
    }
}
