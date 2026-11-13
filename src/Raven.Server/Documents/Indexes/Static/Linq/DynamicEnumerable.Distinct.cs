using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Linq
{
    public static partial class DynamicEnumerable
    {
        // The tuple binds a dynamic source statically to the object overload; a CLR source keeps its element type here. Generated object
        // calls are lowered to Distinct in the index assembly; these overloads remain the fallback when that binding is unavailable.
        public static IEnumerable<TSource> DistinctFromEnumerable<TSource>((IEnumerable<TSource> Source, IEqualityComparer<TSource> Comparer) arguments)
        {
            return Enumerable.Distinct(arguments.Source, arguments.Comparer);
        }

        public static IEnumerable<dynamic> DistinctFromEnumerable((object Source, object Comparer) arguments)
        {
            object distinct = DynamicDistinctFromEnumerable(arguments);
            return DistinctResultAsObjects(distinct);
        }

        public static IEnumerable<dynamic> DistinctResultAsObjects(object distinct)
        {
            return distinct as IEnumerable<object> ?? Enumerable.Cast<object>((IEnumerable)distinct);
        }

        // Selected by GeneratedSourceSelector instead of the object overload when the consumer needs the result with its own element type.
        public static dynamic DynamicDistinctFromEnumerable((object Source, object Comparer) arguments)
        {
            return Enumerable.Distinct((dynamic)arguments.Source, (dynamic)arguments.Comparer);
        }

        public static IEnumerable<TSource> DistinctFromEnumerableComparerFirst<TSource>((IEqualityComparer<TSource> Comparer, IEnumerable<TSource> Source) arguments)
        {
            return DistinctFromEnumerable((arguments.Source, arguments.Comparer));
        }

        public static IEnumerable<dynamic> DistinctFromEnumerableComparerFirst((object Comparer, object Source) arguments)
        {
            return DistinctFromEnumerable((arguments.Source, arguments.Comparer));
        }

        public static dynamic DynamicDistinctFromEnumerableComparerFirst((object Comparer, object Source) arguments)
        {
            return DynamicDistinctFromEnumerable((arguments.Source, arguments.Comparer));
        }
    }
}
