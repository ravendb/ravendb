using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Linq
{
    public static partial class DynamicEnumerable
    {
        public static dynamic Min(IEnumerable source)
        {
            if (source == null) return DynamicNullObject.Null;

            var enumerator = source.GetEnumerator();
            using var disposableEnumerator = enumerator as IDisposable;
            if (enumerator.MoveNext() == false)
                return DynamicNullObject.Null;

            var comparer = Comparer<object>.Default;
            object result;
            do
            {
                result = enumerator.Current;
                if (result != null)
                    break;
            } while (enumerator.MoveNext());

            if (result == null)
                return DynamicNullObject.Null;

            while (enumerator.MoveNext())
            {
                var value = enumerator.Current;
                if (value != null && comparer.Compare(value, result) < 0)
                    result = value;
            }

            return result;
        }

        public static dynamic Min<TSource, TResult>(IEnumerable<TSource> source, Func<TSource, TResult> selector)
        {
            return Min(Enumerable.Select(source, selector));
        }

        public static dynamic Max<TSource>(IEnumerable<TSource> source)
        {
            if (source == null) return DynamicNullObject.Null;

            using var enumerator = source.GetEnumerator();
            if (enumerator.MoveNext() == false)
                return DynamicNullObject.Null;

            var comparer = Comparer<object>.Default;
            // Custom enumerators may expose different values through their non-generic Current.
            IEnumerator untypedEnumerator = enumerator;
            object result;
            do
            {
                result = untypedEnumerator.Current;
                if (result != null)
                    break;
            } while (untypedEnumerator.MoveNext());

            if (result == null)
                return DynamicNullObject.Null;

            while (untypedEnumerator.MoveNext())
            {
                object value = untypedEnumerator.Current;
                if (value != null && comparer.Compare(value, result) > 0)
                    result = value;
            }

            return result;
        }

        public static dynamic Max<TSource, TResult>(IEnumerable<TSource> source, Func<TSource, TResult> selector)
        {
            return Max(Enumerable.Select(source, selector));
        }
    }
}
