using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    internal class PortableExceptions
    {
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

            // We will still throw but in a way that we can look
            throw new NotSupportedException($"Exception type '{typeof(T).Name}' is not supported by this {nameof(Throw)} statement.");
        }

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        public static void Throw<T>(string message)
        {
            if (typeof(T) == typeof(ArgumentException))
                throw new ArgumentException(message);
            if (typeof(T) == typeof(ArgumentNullException))
                throw new ArgumentNullException(null, message);
            if (typeof(T) == typeof(ArgumentOutOfRangeException))
                throw new ArgumentOutOfRangeException(null, message);
            if (typeof(T) == typeof(InvalidOperationException))
                throw new InvalidOperationException(message);

            // We will still throw but in a way that we can look
            throw new NotSupportedException($"Exception type '{typeof(T).Name}' is not supported by this {nameof(Throw)} statement.");
        }


#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private static void Throw(string paramName) => Throw<ArgumentNullException>(paramName);

#if NET6_0_OR_GREATER
        [DoesNotReturn]
#endif
        private static void Throw() => throw new ArgumentNullException();
    }
}
