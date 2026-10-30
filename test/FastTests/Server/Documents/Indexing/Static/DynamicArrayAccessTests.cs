using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class DynamicArrayAccessTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(200)]
        public void CountLengthAndIndexerOverBlittableArray(int numberOfItems)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(CreateDocument(numberOfItems), "doc"))
            {
                Assert.True(doc.TryGet("Items", out BlittableJsonReaderArray items));
                AssertAccess(new DynamicArray(items), numberOfItems);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(10)]
        public void CountLengthAndIndexerOverEnumerable(int numberOfItems)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(CreateDocument(numberOfItems), "doc"))
            {
                Assert.True(doc.TryGet("Items", out BlittableJsonReaderArray items));

                // a LINQ sequence, so the inner collection is not a blittable array
                AssertAccess(new DynamicArray(items.Select(x => x)), numberOfItems);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void ConversionToTypedArrays()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(new DynamicJsonValue
            {
                ["Names"] = new DynamicJsonArray { "a", "bb", "ccc" },
                ["Numbers"] = new DynamicJsonArray { 1, 22, 333 }
            }, "doc"))
            {
                Assert.True(doc.TryGet("Names", out BlittableJsonReaderArray names));
                Assert.True(doc.TryGet("Numbers", out BlittableJsonReaderArray numbers));

                dynamic namesArray = new DynamicArray(names);
                dynamic numbersArray = new DynamicArray(numbers);
                dynamic namesSequence = new DynamicArray(names.Select(x => x));

                Assert.Equal(new[] { "a", "bb", "ccc" }, (string[])namesArray);
                Assert.Equal(new[] { "a", "bb", "ccc" }, (string[])namesSequence);
                Assert.Equal(new[] { 1L, 22L, 333L }, (long[])numbersArray);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void EnumerationDisposesTheInnerEnumerator()
        {
            var inner = new TrackingEnumerable(new object[] { 1L, 2L, 3L });

            var values = new List<object>();
            foreach (var item in new DynamicArray(inner))
                values.Add(item);

            Assert.Equal(new object[] { 1L, 2L, 3L }, values);
            Assert.Equal(1, inner.NumberOfEnumerators);
            Assert.Equal(1, inner.NumberOfDisposedEnumerators);
        }

        private static void AssertAccess(DynamicArray array, int numberOfItems)
        {
            dynamic dynamicArray = array;

            Assert.Equal(numberOfItems, array.Count());
            Assert.Equal(numberOfItems, (int)dynamicArray.Length);
            Assert.Equal(numberOfItems, (int)dynamicArray.Count);

            for (var i = 0; i < numberOfItems; i++)
            {
                Assert.Equal(i, (int)(long)dynamicArray[i].Index);
                Assert.Equal(i, (int)(long)array.ElementAt(i).Index);
                Assert.Equal(i, (int)(long)array[i].Index);
            }

            var position = 0;
            foreach (dynamic item in array)
            {
                Assert.IsType<DynamicBlittableJson>(item);
                Assert.Equal(position++, (int)(long)item.Index);
            }

            Assert.Equal(numberOfItems, position);

            Assert.Throws<ArgumentOutOfRangeException>(() => array.ElementAt(numberOfItems));
            Assert.Throws<ArgumentOutOfRangeException>(() => array.ElementAt(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => (object)dynamicArray[numberOfItems]);
        }

        private static DynamicJsonValue CreateDocument(int numberOfItems)
        {
            var items = new DynamicJsonArray();
            for (var i = 0; i < numberOfItems; i++)
                items.Add(new DynamicJsonValue { ["Index"] = i });

            return new DynamicJsonValue { ["Items"] = items };
        }

        private sealed class TrackingEnumerable(object[] items) : IEnumerable<object>
        {
            public int NumberOfEnumerators;
            public int NumberOfDisposedEnumerators;

            public IEnumerator<object> GetEnumerator()
            {
                NumberOfEnumerators++;
                return new Enumerator(this, items);
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            private sealed class Enumerator(TrackingEnumerable parent, object[] items) : IEnumerator<object>
            {
                private int _index = -1;

                public bool MoveNext() => ++_index < items.Length;

                public void Reset() => _index = -1;

                public object Current => items[_index];

                object IEnumerator.Current => Current;

                public void Dispose() => parent.NumberOfDisposedEnumerators++;
            }
        }
    }
}
