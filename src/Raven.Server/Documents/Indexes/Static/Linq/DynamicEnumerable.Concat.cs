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

        [UsedImplicitly(Reason = "The return value is consumed by generated index code.")]
        public static IEnumerable<dynamic> ConcatFromEnumerable(object first, object second)
        {
            return Concat(first, second);
        }

        // Unwrapping lets Enumerable use the counts and CopyTo of collection operands instead of enumerating them through the wrapper.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CountFromEnumerable(IEnumerable<object> source)
        {
            return Enumerable.Count(ConcatenatedArray.Unwrap(source));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CountFromEnumerable<TSource>(IEnumerable<TSource> source)
        {
            return Enumerable.Count(source);
        }

        public static int CountFromEnumerable(object source)
        {
            // ReSharper disable once PossibleInvalidCastException
            return CountFromEnumerable(source as IEnumerable<object> ?? (IEnumerable<object>)(dynamic)source);
        }

        // Reassigning the parameter would lose its exact type after inlining, and the JIT would no longer fold the collection checks.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static TSource[] ToArrayFromEnumerable<TSource>(IEnumerable<TSource> source)
        {
            if (source is ConcatenatedArray concatenated)
                return Enumerable.ToArray((IEnumerable<TSource>)ConcatenatedArray.Unwrap(concatenated));

            return Enumerable.ToArray(source);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static List<TSource> ToListFromEnumerable<TSource>(IEnumerable<TSource> source)
        {
            if (source is ConcatenatedArray concatenated)
                return Enumerable.ToList((IEnumerable<TSource>)ConcatenatedArray.Unwrap(concatenated));

            return Enumerable.ToList(source);
        }

        private static IEnumerable<dynamic> ConcatDynamicArray(IEnumerable<object> first, IEnumerable<object> second)
        {
            return new ConcatenatedArray(Enumerable.Concat(ConcatenatedArray.Unwrap(first), ConcatenatedArray.Unwrap(second)));
        }

        // A common marker for DynamicArray and DynamicNullObject keeps the overloads unambiguous for a literal null.
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
