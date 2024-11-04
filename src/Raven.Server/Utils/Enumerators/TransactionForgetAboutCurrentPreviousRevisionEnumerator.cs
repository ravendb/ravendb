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

    protected override void ForgetAbout(ForgetAboutItem item)
    {
        if (item.IsCloned)
        {
            using (item.CompressedItem.Current)
            using (item.CompressedItem.Previous)
            {
                DocsContext.Transaction.ForgetAbout(item.CompressedItem.Current);
                DocsContext.Transaction.ForgetAbout(item.CompressedItem.Previous);
            }
        }
    }

    protected override ForgetAboutItem CloneCurrent((Document Previous, Document Current) item)
    {
        if (DocsContext.Transaction.CanForgetAbout(item.Previous) && DocsContext.Transaction.CanForgetAbout(item.Current))
        {
            // this is revision so both items should be compressed
            return new ForgetAboutItem()
            {
                Item = (item.Previous.Clone(DocsContext), item.Current.Clone(DocsContext)),
                CompressedItem = item,
                IsCloned = true
            };
        }

        return new ForgetAboutItem()
        {
            Item = item
        };
    }
}
