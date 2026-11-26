using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Linq
{
    public static partial class DynamicEnumerable
    {
        // The instance method binds first and passes int elements; when the consumer does not bind with them, the compiler falls back
        // to the extension method, which passes objects. GeneratedSourceSelector replaces the chosen call with the selected source.
        public sealed class RangeSource
        {
            public static readonly RangeSource Instance = new RangeSource();

            private static readonly Func<IEnumerable<int>, IEnumerable<int>> IntElements = source => source;

            private RangeSource()
            {
            }

            public TResult ChooseRangeSource<TResult>(Func<Func<IEnumerable<int>, IEnumerable<int>>, TResult> consumer)
            {
                return consumer(IntElements);
            }
        }

        private static readonly Func<IEnumerable<int>, IEnumerable<dynamic>> ObjectElements = source => Enumerable.Cast<dynamic>(source);

        public static TResult ChooseRangeSource<TResult>(this RangeSource range, Func<Func<IEnumerable<int>, IEnumerable<dynamic>>, TResult> consumer)
        {
            return consumer(ObjectElements);
        }
    }
}
