using System.Collections;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Utils.Enumerators;

public class TransactionForgetAboutDocumentEnumerator : IEnumerator<Document>
{
    private readonly IEnumerator<Document> _innerEnumerator;
    private readonly DocumentsOperationContext _docsContext;

    public TransactionForgetAboutDocumentEnumerator([NotNull] IEnumerator<Document> innerEnumerator, [NotNull] DocumentsOperationContext docsContext)
    {
        _innerEnumerator = innerEnumerator;
        _docsContext = docsContext;
    }

    public bool MoveNext()
    {
        _docsContext.Transaction.ForgetAbout(Current);

        if (_innerEnumerator.MoveNext() == false)
            return false;

        Current = _innerEnumerator.Current;

        return true;
    }

    public void Reset()
    {
        throw new System.NotImplementedException();
    }

    public Document Current { get; private set; }

    object IEnumerator.Current => Current;

    public void Dispose()
    {
        _innerEnumerator.Dispose();
    }
}
