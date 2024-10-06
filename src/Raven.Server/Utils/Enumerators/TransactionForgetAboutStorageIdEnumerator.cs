using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using static Voron.Data.Tables.Table;

namespace Raven.Server.Utils.Enumerators;

public class TransactionForgetAboutStorageIdEnumerator : TransactionForgetAboutAbstractEnumerator<SeekResult>
{
    public TransactionForgetAboutStorageIdEnumerator([NotNull] IEnumerator<SeekResult> innerEnumerator, [NotNull] DocumentsOperationContext docsContext) : base(innerEnumerator, docsContext)
    {
    }

    protected override void ForgetAbout(SeekResult item)
    {
        if (item.Result?.Reader != null)
            DocsContext.Transaction.InnerTransaction.ForgetAbout(item.Result.Reader.Id);
    }
}
