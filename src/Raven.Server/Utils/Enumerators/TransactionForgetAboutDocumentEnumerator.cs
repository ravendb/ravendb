using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Utils.Enumerators;

public class TransactionForgetAboutDocumentEnumerator : TransactionForgetAboutAbstractEnumerator<Document>
{
    public TransactionForgetAboutDocumentEnumerator([NotNull] IEnumerator<Document> innerEnumerator, [NotNull] DocumentsOperationContext docsContext) : base(innerEnumerator, docsContext)
    {
    }

    protected override void ForgetAbout(ForgetAboutItem item)
    {
        if (item.IsCloned)
        {
            using (item.CompressedItem)
            {
                DocsContext.Transaction.ForgetAbout(item.CompressedItem);
            }
        }
    }

    protected override ForgetAboutItem CloneCurrent(Document item)
    {
        if (DocsContext.Transaction.CanForgetAbout(item))
        {
            return new ForgetAboutItem()
            {
                Item = item.Clone(DocsContext),
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
