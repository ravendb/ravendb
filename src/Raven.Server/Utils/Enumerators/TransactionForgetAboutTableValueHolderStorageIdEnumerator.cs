using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using static Voron.Data.Tables.Table;

namespace Raven.Server.Utils.Enumerators;

public class TransactionForgetAboutTableValueHolderStorageIdEnumerator : TransactionForgetAboutAbstractEnumerator<TableValueHolder>
{
    public TransactionForgetAboutTableValueHolderStorageIdEnumerator([NotNull] IEnumerator<TableValueHolder> innerEnumerator, [NotNull] DocumentsOperationContext docsContext) : base(innerEnumerator, docsContext)
    {
    }

    protected override void ForgetAbout(TableValueHolder item)
    {
        if (item?.Reader != null)
            DocsContext.Transaction.InnerTransaction.ForgetAbout(item.Reader.Id);
    }
}
