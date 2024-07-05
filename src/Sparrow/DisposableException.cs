using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Sparrow;

public static class DisposableException
{
    [Conditional("DEBUG")]
    public static void ThrowIfDisposedOnDebug<T>(
        T disposable, string message = "The object has already been disposed.",
#if NET6_0_OR_GREATER
        [CallerArgumentExpression(nameof(disposable))]
#endif
        string paramName = null) where T : IDisposableQueryable
    {
        ThrowIfDisposed(disposable, message, paramName);
    }


    public static void ThrowIfDisposed<T>(
        T disposable, string message = "The object has already been disposed.",
#if NET6_0_OR_GREATER        
        [CallerArgumentExpression(nameof(disposable))]
#endif
        string paramName = null) where T : IDisposableQueryable
    {
        if (disposable.IsDisposed)
        {
#if !NET6_0_OR_GREATER
            paramName ??= disposable.GetType().Name;
#endif
            Throw(paramName, message);
        }
    }

    public static void Throw<T>(
        T disposable, string message = "The object has already been disposed.",
#if NET6_0_OR_GREATER
        [CallerArgumentExpression(nameof(disposable))]
#endif
        string paramName = null) where T : IDisposableQueryable
    {
#if !NET6_0_OR_GREATER
        paramName ??= disposable.GetType().Name;
#endif
        Throw(paramName, message);
    }

#if NET6_0_OR_GREATER
    [DoesNotReturn]
#endif
    private static void Throw(string paramName, string message) => throw new ObjectDisposedException(paramName, message);
}
