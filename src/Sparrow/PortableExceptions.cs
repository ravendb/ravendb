using System;
using System.Diagnostics;
using System.IO;

#if NET6_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
#endif

namespace Sparrow
{
    internal class PortableExceptions
    {
        // In order to differ the allocation of interpolations we need to do it through a delegate.
        // More details in the following issue: https://github.com/dotnet/runtime/issues/101996
        // We could remove all the API when this is solved at the JIT level. 
        internal delegate string ThrowMessage();

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static void ThrowIfNull(
#if NET6_0_OR_GREATER              
            [NotNull]
#endif
            object argument,
#if NET6_0_OR_GREATER        
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null)
        {
            if (argument == null)
            {
#if NET6_0_OR_GREATER
                Throw(paramName);
#else
                Throw();
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowIfNullOnDebug(
#if NET6_0_OR_GREATER              
            [NotNull]
#endif
            object argument,
#if NET6_0_OR_GREATER        
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null)
        {
            ThrowIfNull(argument, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static void ThrowIfNotNull(
#if NET6_0_OR_GREATER              
            [NotNull]
#endif
            object argument,
#if NET6_0_OR_GREATER        
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null)
        {
            if (argument != null)
            {
#if NET6_0_OR_GREATER
                Throw(paramName);
#else
                Throw();
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowIfNotNullOnDebug(
#if NET6_0_OR_GREATER              
            [NotNull]
#endif
            object argument,
#if NET6_0_OR_GREATER        
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null)
        {
            ThrowIfNotNull(argument, paramName);
        }

        public static void ThrowIfNull<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            object argument,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            if (argument == null)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message);
#else
                Throw<T>(message);
#endif
            }
        }

        public static void ThrowIfNull<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            object argument,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            if (argument == null)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message());
#else
                Throw<T>(message());
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowIfNullOnDebug<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            object argument,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNull<T>(argument, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static void ThrowIfNotNull<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            object argument,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            if (argument != null)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message);
#else
                Throw<T>(message);
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static void ThrowIfNotNull<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            object argument,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            if (argument != null)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message());
#else
                Throw<T>(message());
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowIfNotNullOnDebug<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            object argument,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNotNull<T>(argument, message, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowIfNotNullOnDebug<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            object argument,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNotNull<T>(argument, message, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static unsafe void ThrowIfNull(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null)
        {
            if (argument == null)
            {
#if NET6_0_OR_GREATER
                Throw(paramName);
#else
                Throw();
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static unsafe void ThrowIfNullOnDebug(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null)
        {
            ThrowIfNull(argument, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static unsafe void ThrowIfNotNull(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null)
        {
            if (argument != null)
            {
#if NET6_0_OR_GREATER
                Throw(paramName);
#else
                Throw();
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static unsafe void ThrowIfNotNullOnDebug(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null)
        {
            ThrowIfNotNull(argument, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static unsafe void ThrowIfNullOnDebug<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNull<T>(argument, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static unsafe void ThrowIfNull<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            if (argument == null)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message);
#else
                Throw<T>(message);
#endif
            }
        }


#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static unsafe void ThrowIfNull<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            if (argument == null)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message());
#else
                Throw<T>(message());
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static unsafe void ThrowIfNullOnDebug<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNull<T>(argument, message, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static unsafe void ThrowIfNullOnDebug<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNull<T>(argument, message(), paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static unsafe void ThrowIfNotNull<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            if (argument != null)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message);
#else
                Throw<T>(message);
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static unsafe void ThrowIfNotNull<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            if (argument != null)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message());
#else
                Throw<T>(message());
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static unsafe void ThrowIfNotNullOnDebug<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNotNull<T>(argument, message, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static unsafe void ThrowIfNotNullOnDebug<T>(
#if NET6_0_OR_GREATER
            [NotNull]
#endif
            void* argument,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(argument))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNotNull<T>(argument, message, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static void ThrowIf<T>(
            bool condition,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(condition))]
#endif
            string paramName = null) where T : Exception
        {
            if (condition)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message);
#else
                Throw<T>(message);
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static void ThrowIf<T>(
            bool condition,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(condition))]
#endif
            string paramName = null) where T : Exception
        {
            if (condition)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message());
#else
                Throw<T>(message());
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowIfOnDebug<T>(
            bool condition,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(condition))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIf<T>(condition, message, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowIfOnDebug<T>(
            bool condition,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(condition))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIf<T>(condition, message, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static void ThrowIfNot<T>(
            bool condition,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(condition))]
#endif
            string paramName = null) where T : Exception
        {
            if (condition == false)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message);
#else
                Throw<T>(message);
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static void ThrowIfNot<T>(
            bool condition,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(condition))]
#endif
            string paramName = null) where T : Exception
        {
            if (condition == false)
            {
#if NET6_0_OR_GREATER
                Throw<T>(paramName, message());
#else
                Throw<T>(message());
#endif
            }
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowIfNotOnDebug<T>(
            bool condition,
            string message = null,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(condition))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNot<T>(condition, message, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowIfNotOnDebug<T>(
            bool condition,
            ThrowMessage message,
#if NET6_0_OR_GREATER
            [CallerArgumentExpression(nameof(condition))]
#endif
            string paramName = null) where T : Exception
        {
            ThrowIfNot<T>(condition, message, paramName);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        public static void Throw<T>(string paramName, string message)
        {
            if (typeof(T) == typeof(ArgumentException))
                throw new ArgumentException(message, paramName);
            if (typeof(T) == typeof(ArgumentNullException))
                throw new ArgumentNullException(paramName, message);
            if (typeof(T) == typeof(ArgumentOutOfRangeException))
                throw new ArgumentOutOfRangeException(paramName, message);
            if (typeof(T) == typeof(InvalidOperationException))
                throw new InvalidOperationException(message);
            if (typeof(T) == typeof(ObjectDisposedException))
                throw new ObjectDisposedException(message);
            if (typeof(T) == typeof(EndOfStreamException))
                throw new EndOfStreamException(message);
            if (typeof(T) == typeof(InvalidDataException))
                throw new InvalidDataException(message);

            // We will still throw but in a way that we can look
            throw new NotSupportedException($"Exception type '{typeof(T).Name}' is not supported by this {nameof(Throw)} statement.");
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowOnDebug<T>(string paramName, string message)
        {
            Throw<T>(paramName, message);
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        public static T Throw<T>(string message) where T : Exception
        {
            if (typeof(T) == typeof(ArgumentException))
                throw new ArgumentException(message);
            if (typeof(T) == typeof(ArgumentNullException))
                throw new ArgumentNullException(null, message);
            if (typeof(T) == typeof(ArgumentOutOfRangeException))
                throw new ArgumentOutOfRangeException(null, message);
            if (typeof(T) == typeof(InvalidOperationException))
                throw new InvalidOperationException(message);
            if (typeof(T) == typeof(ObjectDisposedException))
                throw new ObjectDisposedException(message);
            if (typeof(T) == typeof(InvalidDataException))
                throw new InvalidDataException(message);

            // We will still throw but in a way that we can look
            throw new NotSupportedException($"Exception type '{typeof(T).Name}' is not supported by this {nameof(Throw)} statement.");
        }

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        [Conditional("DEBUG")]
        public static void ThrowOnDebug<T>(string message) where T : Exception
        {
            Throw<T>(message);
        }


#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private static void Throw(string paramName) => Throw<ArgumentNullException>(paramName);

#if NETCOREAPP2_0_OR_GREATER
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private static void Throw() => throw new ArgumentNullException();
    }
}
