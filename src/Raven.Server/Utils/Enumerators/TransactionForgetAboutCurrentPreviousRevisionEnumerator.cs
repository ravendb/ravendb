using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Utils.Enumerators;

public class TransactionForgetAboutCurrentPreviousRevisionEnumerator : TransactionForgetAboutAbstractEnumerator<(Document Previous, Document Current)>
{
    public TransactionForgetAboutCurrentPreviousRevisionEnumerator([NotNull] IEnumerator<(Document Previous, Document Current)> innerEnumerator, [NotNull] DocumentsOperationContext docsContext) : base(innerEnumerator, docsContext)
    {
    }

    protected override void ForgetAbout((Document Previous, Document Current) item)
    {
        DocsContext.Transaction.ForgetAbout(item.Current);
        DocsContext.Transaction.ForgetAbout(item.Previous);
    }
}
