using System;
using System.Diagnostics;
using System.IO;
using System.Text;

#if NET6_0_OR_GREATER
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
#endif

namespace Sparrow
{
    internal class PortableExceptions
    {

#if NET6_0_OR_GREATER         
        // In order to defer the allocation of interpolations we need to do it through a InterpolatedStringHandler.
        // More details in the following issue: https://github.com/dotnet/runtime/issues/101996
        // We could remove all the API when this is solved at the JIT level. 

        [InterpolatedStringHandler]
        public readonly ref struct ConditionalThrowInterpolatedStringHandler
        {
            private readonly StringBuilder? _builder;

            public ConditionalThrowInterpolatedStringHandler(int literalLength, int formattedCount, bool condition, out bool isEnabled)
            {
                isEnabled = condition;
                _builder = condition ? new StringBuilder(literalLength + formattedCount * 2 + 1) : null;
            }

            public void AppendLiteral(string s)
            {
                _builder!.Append(s);
            }

            public void AppendFormatted<T>(T t)
            {
                _builder!.Append(t);
            }

            internal string? GetFormattedText()
            {
                return _builder!.ToString();
            }
        }

        [InterpolatedStringHandler]
        public readonly ref struct NotConditionalThrowInterpolatedStringHandler
        {
            private readonly StringBuilder? _builder;

            public NotConditionalThrowInterpolatedStringHandler(int literalLength, int formattedCount, bool condition, out bool isEnabled)
            {
                isEnabled = condition == false;
                _builder = condition == false ? new StringBuilder(literalLength + formattedCount * 2 + 1) : null;
            }

            public void AppendLiteral(string s)
            {
                _builder!.Append(s);
            }

            public void AppendFormatted<T>(T t)
            {
                _builder!.Append(t);
            }

            internal string? GetFormattedText()
            {
                return _builder!.ToString();
            }
        }

        [InterpolatedStringHandler]
        public readonly ref struct NullThrowInterpolatedStringHandler
        {
            private readonly StringBuilder? _builder;

            public NullThrowInterpolatedStringHandler(int literalLength, int formattedCount, object condition, out bool isEnabled)
            {
                isEnabled = condition == null;
                _builder = condition == null ? new StringBuilder(literalLength + formattedCount * 2 + 1) : null;
            }

            public void AppendLiteral(string s)
            {
                _builder!.Append(s);
            }

            public void AppendFormatted<T>(T t)
            {
                _builder!.Append(t);
            }

            internal string? GetFormattedText()
            {
                return _builder!.ToString();
            }
        }

        [InterpolatedStringHandler]
        public readonly ref struct NotNullThrowInterpolatedStringHandler
        {
            private readonly StringBuilder? _builder;

            public NotNullThrowInterpolatedStringHandler(int literalLength, int formattedCount, object condition, out bool isEnabled)
            {
                isEnabled = condition != null;
                _builder = condition != null ? new StringBuilder(literalLength + formattedCount * 2 + 1) : null;
            }

            public void AppendLiteral(string s)
            {
                _builder!.Append(s);
            }

            public void AppendFormatted<T>(T t)
            {
                _builder!.Append(t);
            }

            internal string? GetFormattedText()
            {
                return _builder!.ToString();
            }
        }

#endif        

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


#if NET6_0_OR_GREATER         
        public static void ThrowIfNull<T>(
            [NotNull]
            object argument,
            [InterpolatedStringHandlerArgument(nameof(argument))]
            NullThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(argument))]
            string paramName = null) where T : Exception
        {
            if (argument == null)
            {
                Throw<T>(paramName, message.GetFormattedText());
            }
        }
#endif

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

#if NET6_0_OR_GREATER 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNotNull<T>(
            [NotNull]
            object argument,
            [InterpolatedStringHandlerArgument(nameof(argument))]
            NotNullThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(argument))]
            string paramName = null) where T : Exception
        {
            if (argument != null)
            {
                Throw<T>(paramName, message.GetFormattedText());
            }
        }
#endif

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

#if NET6_0_OR_GREATER         
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [Conditional("DEBUG")]
        public static void ThrowIfNotNullOnDebug<T>(
            [NotNull]
            object argument,
            [InterpolatedStringHandlerArgument(nameof(argument))]
            NotNullThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(argument))]
            string paramName = null) where T : Exception
        {
            ThrowIfNotNull<T>(argument, message, paramName);
        }
#endif

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


#if NET6_0_OR_GREATER 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void ThrowIfNull<T>(

            [NotNull]
            void* argument,
            [InterpolatedStringHandlerArgument(nameof(argument))]
            NullThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(argument))]
            string paramName = null) where T : Exception
        {
            if (argument == null)
            {
                Throw<T>(paramName, message.GetFormattedText());
            }
        }
#endif

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

#if NET6_0_OR_GREATER         
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [Conditional("DEBUG")]
        public static unsafe void ThrowIfNullOnDebug<T>(
            [NotNull]
            void* argument,
            [InterpolatedStringHandlerArgument(nameof(argument))]
            NullThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(argument))]
            string paramName = null) where T : Exception
        {
            ThrowIfNull<T>(argument, message.GetFormattedText(), paramName);
        }
#endif
        
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

#if NET6_0_OR_GREATER 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void ThrowIfNotNull<T>(
            [NotNull]
            void* argument,
            [InterpolatedStringHandlerArgument(nameof(argument))]
            NotNullThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(argument))]
            string paramName = null) where T : Exception
        {
            if (argument != null)
            {
                Throw<T>(paramName, message.GetFormattedText());
            }
        }
#endif

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

#if NET6_0_OR_GREATER 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [Conditional("DEBUG")]
        public static unsafe void ThrowIfNotNullOnDebug<T>(
            [NotNull]
            void* argument,
            [InterpolatedStringHandlerArgument(nameof(argument))]
            NotNullThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(argument))]
            string paramName = null) where T : Exception
        {
            ThrowIfNotNull<T>(argument, message, paramName);
        }
#endif

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

#if NET6_0_OR_GREATER 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIf<T>(
            bool condition,
            [InterpolatedStringHandlerArgument(nameof(condition))]
            ConditionalThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(condition))]
            string paramName = null) where T : Exception
        {
            if (condition)
            {
                Throw<T>(paramName, message.GetFormattedText());
            }
        }
#endif
        
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

#if NET6_0_OR_GREATER 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [Conditional("DEBUG")]
        public static void ThrowIfOnDebug<T>(
            bool condition,
            [InterpolatedStringHandlerArgument(nameof(condition))]
            ConditionalThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(condition))]
            string paramName = null) where T : Exception
        {
            ThrowIf<T>(condition, message, paramName);
        }
#endif

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

#if NET6_0_OR_GREATER 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ThrowIfNot<T>(
            bool condition,
            [InterpolatedStringHandlerArgument(nameof(condition))]
            NotConditionalThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(condition))]
            string paramName = null) where T : Exception
        {
            if (condition == false)
            {
                Throw<T>(paramName, message.GetFormattedText());
            }
        }
#endif
        
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

#if NET6_0_OR_GREATER 
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [Conditional("DEBUG")]
        public static void ThrowIfNotOnDebug<T>(
            bool condition,
            [InterpolatedStringHandlerArgument(nameof(condition))]
            NotConditionalThrowInterpolatedStringHandler message,
            [CallerArgumentExpression(nameof(condition))]
            string paramName = null) where T : Exception
        {
            ThrowIfNot<T>(condition, message, paramName);
        }
#endif
        
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
