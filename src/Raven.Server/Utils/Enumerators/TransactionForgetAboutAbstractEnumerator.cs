using System.Collections;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Utils.Enumerators;

public abstract class TransactionForgetAboutAbstractEnumerator<T> : IEnumerator<T>
{
    private readonly IEnumerator<T> _innerEnumerator;
    protected readonly DocumentsOperationContext DocsContext;

    protected TransactionForgetAboutAbstractEnumerator([NotNull] IEnumerator<T> innerEnumerator, [NotNull] DocumentsOperationContext docsContext)
    {
        _innerEnumerator = innerEnumerator;
        DocsContext = docsContext;
    }

    protected abstract void ForgetAbout(T item);

    public bool MoveNext()
    {
        ForgetAbout(Current);

        if (_innerEnumerator.MoveNext() == false)
            return false;

        Current = _innerEnumerator.Current;

        return true;
    }

    public void Reset()
    {
        throw new System.NotImplementedException();
    }

    public T Current { get; private set; }

    object IEnumerator.Current => Current;

    public void Dispose()
    {
        _innerEnumerator.Dispose();
    }
}
