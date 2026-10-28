using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Indexes.Static.Linq
{
    public static partial class DynamicEnumerable
    {
        public static IEnumerable<dynamic> Concat(object source, object other)
        {
            return new DynamicArray(AsEnumerable(source).Cast<object>().Concat(AsEnumerable(other).Cast<object>()));
        }

        // Enumerable.Concat calls in index code are rewritten here.
        // The generic overload preserves TSource; IDynamicSequence overloads return IEnumerable<dynamic> for further index expressions.
        public static IEnumerable<TSource> ConcatFromEnumerable<TSource>(IEnumerable<TSource> first, IEnumerable<TSource> second)
        {
            return Enumerable.Concat(first, second);
        }

        public static IEnumerable<dynamic> ConcatFromEnumerable(IDynamicSequence first, IEnumerable<object> second)
        {
            return ConcatDynamicArray(first, second);
        }

        public static IEnumerable<dynamic> ConcatFromEnumerable(IEnumerable<object> first, IDynamicSequence second)
        {
            return ConcatDynamicArray(first, second);
        }

        // Without this overload, the two mixed overloads would be ambiguous for two marked operands.
        public static IEnumerable<dynamic> ConcatFromEnumerable(IDynamicSequence first, IDynamicSequence second)
        {
            return ConcatDynamicArray(first, second);
        }

        // Keep the existing dynamic route for operands without a common generic element type.
        [UsedImplicitly(Reason = "The return value is consumed by generated index code.")]
        public static IEnumerable<dynamic> ConcatFromEnumerable(object first, object second)
        {
            return Concat(first, second);
        }

        // Count/ToArray/ToList rewrite targets pass the underlying Concat iterator, so Enumerable can use
        // collection operands' counts and CopyTo instead of enumerating them through the wrapper.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CountFromEnumerable(IEnumerable<object> source)
        {
            return Enumerable.Count(ConcatenatedArray.Unwrap(source));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TSource[] ToArrayFromEnumerable<TSource>(IEnumerable<TSource> source)
        {
            if (typeof(TSource) == typeof(object) && source is ConcatenatedArray concatenated)
                source = (IEnumerable<TSource>)ConcatenatedArray.Unwrap(concatenated);

            return Enumerable.ToArray(source);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static List<TSource> ToListFromEnumerable<TSource>(IEnumerable<TSource> source)
        {
            if (typeof(TSource) == typeof(object) && source is ConcatenatedArray concatenated)
                source = (IEnumerable<TSource>)ConcatenatedArray.Unwrap(concatenated);

            return Enumerable.ToList(source);
        }

        private static IEnumerable<dynamic> ConcatDynamicArray(IEnumerable<object> first, IEnumerable<object> second)
        {
            // Unwrap nested results so Enumerable.Concat can flatten the chain.
            return new ConcatenatedArray(Enumerable.Concat(ConcatenatedArray.Unwrap(first), ConcatenatedArray.Unwrap(second)));
        }

        // Common marker for DynamicArray and DynamicNullObject avoids competing overloads
        // for literal null. DynamicNullObject enumerates as empty; CLR null still throws.
        public interface IDynamicSequence : IEnumerable<object>
        {
        }

        // Preserve CLR elements: DynamicArray's normal enumeration can turn date strings into DateTime.
        private sealed class ConcatenatedArray : DynamicArray, IEnumerable<object>
        {
            private readonly IEnumerable<object> _values;

            public ConcatenatedArray(IEnumerable<object> values) : base(values)
            {
                _values = values;
            }

            IEnumerator<object> IEnumerable<object>.GetEnumerator() => _values.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => _values.GetEnumerator();

            public static IEnumerable<object> Unwrap(IEnumerable<object> source) =>
                source is ConcatenatedArray concatenatedArray
                    ? concatenatedArray._values
                    : source;
        }
    }
}
