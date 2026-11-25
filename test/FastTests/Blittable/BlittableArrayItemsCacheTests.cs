using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Blittable
{
    public class BlittableArrayItemsCacheTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(1)]
        [InlineData(10)]
        [InlineData(128)]
        [InlineData(129)]
        [InlineData(1000)]
        public void NestedItemsAreCachedAndReturnTheSameInstance(int numberOfItems)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(CreateDocument(numberOfItems), "doc"))
            {
                Assert.True(doc.TryGet("Items", out BlittableJsonReaderArray items));
                Assert.Equal(numberOfItems, items.Length);

                // access out of order first, so the cache is filled sparsely
                var last = items[numberOfItems - 1];
                Assert.Same(last, items[numberOfItems - 1]);

                for (var i = 0; i < numberOfItems; i++)
                {
                    var item = (BlittableJsonReaderObject)items[i];
                    Assert.Same(item, items[i]);
                    Assert.Same(item, items.GetValueTokenTupleByIndex(i).Item1);
                    Assert.Equal(BlittableJsonToken.StartObject, items.GetValueTokenTupleByIndex(i).Item2);

                    Assert.True(item.TryGet("Index", out int index));
                    Assert.Equal(i, index);
                }

                var position = 0;
                foreach (var item in items)
                    Assert.Same(items[position++], item);

                Assert.Equal(numberOfItems, position);
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(10)]
        [InlineData(1000)]
        public void PrimitiveItemsInMixedArrayAreReadCorrectly(int numberOfItems)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var array = new DynamicJsonArray();
                for (var i = 0; i < numberOfItems; i++)
                {
                    if (i % 2 == 0)
                        array.Add(new DynamicJsonValue { ["Index"] = i });
                    else
                        array.Add("item/" + i);
                }

                using (var doc = ctx.ReadObject(new DynamicJsonValue { ["Items"] = array }, "doc"))
                {
                    Assert.True(doc.TryGet("Items", out BlittableJsonReaderArray items));

                    for (var round = 0; round < 2; round++)
                    {
                        for (var i = 0; i < numberOfItems; i++)
                        {
                            if (i % 2 == 0)
                            {
                                Assert.Same(items[i], items[i]);
                                Assert.True(((BlittableJsonReaderObject)items[i]).TryGet("Index", out int index));
                                Assert.Equal(i, index);
                            }
                            else
                            {
                                Assert.Equal("item/" + i, items[i].ToString());
                                Assert.Equal(BlittableJsonToken.String, items.GetValueTokenTupleByIndex(i).Item2);
                            }
                        }
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(10)]
        [InlineData(1000)]
        public void NestedItemsAreNotCachedWhenNoCacheIsSet(int numberOfItems)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(CreateDocument(numberOfItems), "doc"))
            {
                doc.NoCache = true;

                Assert.True(doc.TryGet("Items", out BlittableJsonReaderArray items));
                Assert.True(items.NoCache);

                var first = (BlittableJsonReaderObject)items[0];
                var second = (BlittableJsonReaderObject)items[0];

                Assert.NotSame(first, second);
                Assert.True(first.NoCache);
                Assert.True(second.TryGet("Index", out int index));
                Assert.Equal(0, index);
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(10)]
        [InlineData(1000)]
        public void AccessingOutOfRangeItemThrows(int numberOfItems)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(CreateDocument(numberOfItems), "doc"))
            {
                Assert.True(doc.TryGet("Items", out BlittableJsonReaderArray items));

                // fill the cache first, so the lookup in it is exercised as well
                _ = items[0];

                Assert.Throws<IndexOutOfRangeException>(() => items[numberOfItems]);
                Assert.Throws<IndexOutOfRangeException>(() => items[-1]);
            }
        }

        private static DynamicJsonValue CreateDocument(int numberOfItems)
        {
            var array = new DynamicJsonArray();
            for (var i = 0; i < numberOfItems; i++)
                array.Add(new DynamicJsonValue { ["Index"] = i, ["Name"] = "item/" + i });

            return new DynamicJsonValue { ["Items"] = array };
        }
    }
}
