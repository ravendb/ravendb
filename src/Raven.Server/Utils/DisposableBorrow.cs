using System;
using System.Diagnostics.CodeAnalysis;

namespace Raven.Server.Utils;

/// <summary>
/// A disposable allowing to steal the underlying disposable using <see cref="Borrow"/>.
/// </summary>
/// <remarks>
/// It's a ref struct as the scoping with it is meant only for the synchronous execution or async with ValueTask handling.
/// </remarks>
public ref struct DisposableBorrow<T> : IDisposable 
    where T : class, IDisposable
{
    private T _actual;

    public DisposableBorrow(T actual)
    {
        _actual = actual;
    }

    public T Borrow()
    {
        var value=  _actual;
        _actual = null;
        return value;
    }

    public void Dispose() => _actual?.Dispose();
}

public static class DisposableBorrowExtensions
{
    /// <summary>
    /// Turns a disposable into a scope allowing borrows with <see cref="DisposableBorrow{T}.Borrow"/>
    /// </summary>
    public static DisposableBorrow<T> AllowBorrow<T>(this T @this)
        where T : class, IDisposable =>
        new(@this);

    /// <summary>
    /// Turns a disposable into a scope allowing borrows with <see cref="DisposableBorrow{T}.Borrow"/>
    /// </summary>
    public static DisposableBorrow<T> AllowBorrow<T>(this T @this, [MaybeNull] out DisposableBorrow<T> disposableBorrow)
        where T : class, IDisposable =>
        disposableBorrow = new(@this);
}
