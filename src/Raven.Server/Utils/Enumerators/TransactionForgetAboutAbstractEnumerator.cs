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

    protected abstract void ForgetAbout(ForgetAboutItem item);
    protected abstract ForgetAboutItem CloneCurrent(T item);

    protected struct ForgetAboutItem
    {
        public T Item;
        public T CompressedItem;
        public bool IsCloned;
    }

    public bool MoveNext()
    {
        ForgetAbout(_current);

        if (_innerEnumerator.MoveNext() == false)
            return false;

        // the clone is needed because we are going to forget about the _current document
        // Current should be disposed by the caller
        _current = CloneCurrent(_innerEnumerator.Current);
        Current = _current.Item;

        return true;
    }

    public void Reset()
    {
        throw new System.NotImplementedException();
    }

    private ForgetAboutItem _current = new ForgetAboutItem();
    public T Current { get; private set; }

    object IEnumerator.Current => Current;

    public void Dispose()
    {
        _innerEnumerator.Dispose();
    }
}
