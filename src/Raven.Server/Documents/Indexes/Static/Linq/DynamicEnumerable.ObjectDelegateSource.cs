using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Linq
{
    public static partial class DynamicEnumerable
    {
        // Marks the source of a nested Enumerable call; GeneratedSourceSelector replaces it with the source or with PrepareSourceForObjectDelegate.
        public static TSource ObjectDelegateSource<TSource>(TSource source)
        {
            return source;
        }

        public static dynamic PrepareSourceForObjectDelegate(object source)
        {
            if (source is IEnumerable<object> or IDynamicMetaObjectProvider)
                return source;

            return source is IEnumerable sequence
                ? Enumerable.Cast<object>(sequence)
                : source;
        }
    }
}
