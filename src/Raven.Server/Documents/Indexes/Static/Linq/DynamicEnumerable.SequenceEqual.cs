using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Raven.Server.Documents.Indexes.Static.Linq
{
    public static partial class DynamicEnumerable
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool SequenceEqual<TSource>(TSource[] first, TSource[] second)
        {
            if (typeof(TSource) != typeof(object))
                return Enumerable.SequenceEqual(first, second);

            ArgumentNullException.ThrowIfNull(first);
            ArgumentNullException.ThrowIfNull(second);

            // TSource is object here, but the compiler does not narrow the generic type after the guard.
            // The intermediate object cast enables the array cast without copying or boxing elements.
            return ArraysSequenceEqual((object[])(object)first, (object[])(object)second);
        }

        public static bool SequenceEqual<TSource>(IEnumerable<TSource> first, IEnumerable<TSource> second)
        {
            if (first is DynamicArray array)
                return array.SequenceEqual(second);

            if (second is DynamicArray otherArray)
                return DynamicArray.SequencesEqual(first?.Cast<object>(), otherArray);

            if (typeof(TSource) == typeof(object))
                return Enumerable.SequenceEqual((IEnumerable<object>)first, (IEnumerable<object>)second, BlittableTextEqualityComparer.Instance);

            return Enumerable.SequenceEqual(first, second);
        }

        public static bool SequenceEqual(object first, object second)
        {
            if (first is not IEnumerable firstEnumerable || second is not IEnumerable secondEnumerable)
                return false;

            if (first is DynamicArray array)
                return array.SequenceEqual(secondEnumerable);

            if (second is DynamicArray otherArray)
                return DynamicArray.SequencesEqual(firstEnumerable.Cast<object>(), otherArray);

            return Enumerable.SequenceEqual(firstEnumerable.Cast<object>(), secondEnumerable.Cast<object>(), BlittableTextEqualityComparer.Instance);
        }

        private static bool ArraysSequenceEqual(object[] first, object[] second)
        {
            if (first.Length != second.Length)
                return false;

            for (int i = 0; i < first.Length; i++)
            {
                if (BlittableTextEqualityComparer.Instance.Equals(first[i], second[i]) == false)
                    return false;
            }

            return true;
        }

        private sealed class BlittableTextEqualityComparer : IEqualityComparer<object>
        {
            public static readonly BlittableTextEqualityComparer Instance = new();

            // Implements comparer equality; hiding object.Equals is intentional.
            // ReSharper disable once MemberHidesStaticFromOuterClass
            public new bool Equals(object x, object y)
            {
                if (x == null)
                    return y is null or DynamicNullObject;

                return y != null && x.Equals(y) || DynamicArray.EqualsBlittableTextOrNull(x, y);
            }

            public int GetHashCode(object obj)
            {
                // This comparer is used only for pairwise equality; hashed operations have a separate contract.
                throw new NotSupportedException();
            }
        }
    }
}
