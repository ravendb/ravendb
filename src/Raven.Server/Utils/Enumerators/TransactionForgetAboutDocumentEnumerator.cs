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

    protected override void ForgetAbout(Document item)
    {
        DocsContext.Transaction.ForgetAbout(Current);
    }
}
