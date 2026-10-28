using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.Linq
{
    public static partial class DynamicEnumerable
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contains<TSource>(IEnumerable<TSource> source, TSource value)
        {
            if (typeof(TSource) == typeof(object))
            {
                if (source is DynamicArray array and not ICollection<object>)
                    return array.ContainsFromEnumerable(value);

                if (NeedsTextOrNullComparison(value) && UsesDefaultCollectionEquality(source))
                    return ContainsBlittableTextOrNull((IEnumerable<object>)source, value, useCollectionFastPath: true);
            }

            return Enumerable.Contains(source, value);
        }

        public static bool Contains(object source, object value)
        {
            if (source is DynamicArray array)
                return array.Contains(value);

            if (source is not IEnumerable enumerable)
                return false;

            var values = enumerable.Cast<object>();
            if (NeedsTextOrNullComparison(value) && UsesDefaultCollectionEquality(values))
                return ContainsBlittableTextOrNull(values, value, useCollectionFastPath: true);

            return Enumerable.Contains(values, value);
        }

        // Select the array loop at compilation so lists and iterators keep their existing route.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contains<TSource>(TSource[] source, TSource value, IEqualityComparer<TSource> comparer)
        {
            if (typeof(TSource) != typeof(char))
                return Contains((IEnumerable<TSource>)source, value, comparer);

            ArgumentNullException.ThrowIfNull(source);
            if (source.Length == 0)
                return false;

            comparer ??= EqualityComparer<TSource>.Default;
            foreach (TSource item in source)
            {
                if (comparer.Equals(item, value))
                    return true;
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contains<TSource>(IEnumerable<TSource> source, TSource value, IEqualityComparer<TSource> comparer)
        {
            if (typeof(TSource) == typeof(object))
            {
                if (source is DynamicArray array and not ICollection<object> &&
                    (comparer == null || ReferenceEquals(comparer, EqualityComparer<TSource>.Default)))
                    return array.ContainsFromEnumerable(value);

                if (comparer == null && NeedsTextOrNullComparison(value))
                    return ContainsBlittableTextOrNull((IEnumerable<object>)source, value, useCollectionFastPath: false);
            }

            return Enumerable.Contains(source, value, comparer);
        }

        public static bool Contains(object source, object value, IEqualityComparer comparer)
        {
            if (source is DynamicArray array)
                return array.Contains(value, comparer);

            if (source is not IEnumerable enumerable)
                return false;

            if (comparer == null)
            {
                if (NeedsTextOrNullComparison(value))
                    return ContainsBlittableTextOrNull(enumerable.Cast<object>(), value, useCollectionFastPath: false);

                return Enumerable.Contains(enumerable.Cast<object>(), value, comparer: null);
            }

            var isCompareCharacters = comparer is IEqualityComparer<char>;
            foreach (object item in enumerable)
            {
                if (comparer.Equals(DynamicArray.ConvertComparerValue(item, isCompareCharacters), DynamicArray.ConvertComparerValue(value, isCompareCharacters)))
                    return true;
            }

            return false;
        }

        private static bool NeedsTextOrNullComparison(object value)
        {
            return value is null
                or char
                or DynamicNullObject
                or LazyStringValue
                or LazyCompressedStringValue;
        }

        private static bool UsesDefaultCollectionEquality([NoEnumeration] object source)
        {
            // Preserve ICollection.Contains overrides, including a HashSet's custom comparer.
            return source is not ICollection<object> ||
                   source is object[] ||
                   source.GetType() == typeof(List<object>);
        }

        private static bool ContainsBlittableTextOrNull(IEnumerable<object> source, object value, bool useCollectionFastPath)
        {
            ArgumentNullException.ThrowIfNull(source);

            if (useCollectionFastPath)
            {
                if (source is object[] array)
                {
                    if (value != null)
                        return ContainsBlittableTextOrNull(new ReadOnlySpan<object>(array), value);
                    if (array.Length == 0)
                        return false;
                    if (array[0] is null or DynamicNullObject)
                        return true;
                    if (array.Length == 1)
                        return false;

                    return ContainsBlittableNull(new ReadOnlySpan<object>(array, 1, array.Length - 1));
                }

                if (source.GetType() == typeof(List<object>))
                {
                    var span = CollectionsMarshal.AsSpan((List<object>)source);
                    return value == null
                        ? ContainsBlittableNull(span)
                        : ContainsBlittableTextOrNull(span, value);
                }
            }

            string character = null;
            foreach (object item in source)
            {
                if (EqualsBlittableValue(item, value, ref character))
                    return true;
            }

            return false;
        }

        private static bool ContainsBlittableTextOrNull(ReadOnlySpan<object> source, object value)
        {
            string character = null;
            if (value is char c)
            {
                foreach (object item in source)
                {
                    if (item is char other)
                    {
                        if (other == c)
                            return true;
                    }
                    else
                    {
                        if (EqualityComparer<object>.Default.Equals(item, value))
                            return true;
                        if (item is LazyStringValue or LazyCompressedStringValue && item.Equals(character ??= c.ToString()))
                            return true;
                    }
                }

                return false;
            }

            foreach (object item in source)
            {
                if (EqualsBlittableValue(item, value, ref character))
                    return true;
            }

            return false;
        }

        private static bool ContainsBlittableNull(ReadOnlySpan<object> source)
        {
            if (source.IsEmpty)
                return false;
            if (source[0] is null or DynamicNullObject)
                return true;
            if (source.Length == 1)
                return false;

            source = source.Slice(1);
            int i = 0;
            for (; i <= source.Length - 2; i += 2)
            {
                if (source[i] is null or DynamicNullObject)
                    return true;
                if (source[i + 1] is null or DynamicNullObject)
                    return true;
            }

            for (; i < source.Length; i++)
            {
                if (source[i] is null or DynamicNullObject)
                    return true;
            }

            return false;
        }

        private static bool EqualsBlittableValue(object item, object value, ref string character)
        {
            if (EqualityComparer<object>.Default.Equals(item, value))
                return true;

            if (value is char c && item is LazyStringValue or LazyCompressedStringValue)
                return item.Equals(character ??= c.ToString());

            return DynamicArray.EqualsBlittableTextOrNull(item, value);
        }
    }
}
