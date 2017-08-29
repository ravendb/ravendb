using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Sparrow.Collections;
using Xunit;

namespace FastTests.Sparrow
{
    public class FastDictionaryTests : NoDisposalNeeded
    {
        [Fact]
        public void Construction()
        {
            var dict = new FastDictionary<int, int>();
            Assert.Equal(0, dict.Count);
            Assert.Equal(32, dict.Capacity);
            Assert.NotNull(dict.Comparer);

            dict = new FastDictionary<int, int>(null as IEqualityComparer<int>);
            Assert.Equal(0, dict.Count);
            Assert.Equal(32, dict.Capacity);
            Assert.NotNull(dict.Comparer);

            dict = new FastDictionary<int, int>(16);
            Assert.Equal(0, dict.Count);
            Assert.Equal(16, dict.Capacity);
            Assert.NotNull(dict.Comparer);
        }

        [Fact]
        public void ConstructionWithNonPowerOf2()
        {
            var dict = new FastDictionary<int, int>(5);
            Assert.Equal(0, dict.Count);
            Assert.Equal(8, dict.Capacity);
            Assert.NotNull(dict.Comparer);
        }


        [Fact]
        public void ConstructionWithExplicitZeroAndNegative()
        {
            var dict = new FastDictionary<int, int>(0);
            Assert.Equal(0, dict.Count);
            Assert.Equal(8, dict.Capacity);
            Assert.NotNull(dict.Comparer);

            dict = new FastDictionary<int, int>(-1);
            Assert.Equal(0, dict.Count);
            Assert.Equal(8, dict.Capacity);
            Assert.NotNull(dict.Comparer);
        }

        [Fact]
        public void ConstructionWithFastDictionary()
        {
            var dict = new FastDictionary<int, int>(200, EqualityComparer<int>.Default);
            for (int i = 0; i < 100; i++)
                dict[i] = i;

            var fromFastDictionary = new FastDictionary<int, int>(dict.Capacity, dict, EqualityComparer<int>.Default);
            Assert.Equal(dict.Count, fromFastDictionary.Count);
            Assert.Equal(dict.Capacity, fromFastDictionary.Capacity);
            Assert.Equal(dict.Comparer, fromFastDictionary.Comparer);

            int count = 0;
            foreach (var item in fromFastDictionary)
            {
                Assert.Equal(item.Key, item.Value);
                count++;
            }
            Assert.Equal(100, count);
        }

        private class CustomIntEqualityComparer : EqualityComparer<int>
        {
            public override bool Equals(int x, int y)
            {
                return x == y;
            }

            public override int GetHashCode(int obj)
            {
                return obj;
            }
        }

        [Fact]
        public void ConstructionWithFastDictionaryAndDifferentComparer()
        {
            var equalityComparer = new CustomIntEqualityComparer();

            var dict = new FastDictionary<int, int>(200, equalityComparer);
            for (int i = 0; i < 100; i++)
                dict[i] = i;

            var fromFastDictionary = new FastDictionary<int, int>(dict, EqualityComparer<int>.Default);
            Assert.Equal(dict.Count, fromFastDictionary.Count);
            Assert.Equal(dict.Capacity, fromFastDictionary.Capacity);
            Assert.NotSame(dict.Comparer, fromFastDictionary.Comparer);

            int count = 0;
            foreach (var item in fromFastDictionary)
            {
                Assert.Equal(item.Key, item.Value);
                count++;
            }
            Assert.Equal(100, count);
        }


        [Fact]
        public void ConstructionWithNativeDictionary()
        {
            var dict = new Dictionary<int, int>(200, EqualityComparer<int>.Default);
            for (int i = 0; i < 100; i++)
                dict[i] = i;

            var fromFastDictionary = new FastDictionary<int, int>(dict.Count, dict, EqualityComparer<int>.Default);
            Assert.Equal(dict.Count, fromFastDictionary.Count);
            Assert.Equal(dict.Comparer, fromFastDictionary.Comparer);

            int count = 0;
            foreach (var item in fromFastDictionary)
            {
                Assert.Equal(item.Key, item.Value);
                count++;
            }
            Assert.Equal(100, count);
        }



        [Fact]
        public void ConsecutiveInsertionsWithIndexerAndWithoutGrow()
        {
            var dict = new FastDictionary<int, int>(200);

            for (int i = 0; i < 100; i++)
                dict[i] = i;

            for (int i = 0; i < 100; i++)
            {
                Assert.True(dict.Contains(i));
                Assert.Equal(i, dict[i]);
            }

            int count = 0;
            foreach (var item in dict)
            {
                Assert.Equal(item.Key, item.Value);
                count++;
            }
            Assert.Equal(100, count);

            Assert.Equal(100, dict.Count);
            Assert.Equal(256, dict.Capacity);
        }


        [Fact]
        public void ConsecutiveInsertionsWithIndexerAndGrow()
        {
            var dict = new FastDictionary<int, int>(4);

            for (int i = 0; i < 100; i++)
                dict[i] = i;

            for (int i = 0; i < 100; i++)
            {
                Assert.True(dict.Contains(i));
                Assert.Equal(i, dict[i]);
            }

            int count = 0;
            foreach (var item in dict)
            {
                Assert.Equal(item.Key, item.Value);
                count++;
            }
            Assert.Equal(100, count);


            Assert.Equal(100, dict.Count);
            Assert.Equal(256, dict.Capacity);
        }

        [Fact]
        public void ConsecutiveInsertionsWithoutGrow()
        {
            var dict = new FastDictionary<int, int>(200);

            for (int i = 0; i < 100; i++)
                dict.Add(i, i);

            for (int i = 0; i < 100; i++)
            {
                Assert.True(dict.Contains(i));
                Assert.Equal(i, dict[i]);
            }

            int count = 0;
            foreach (var item in dict)
            {
                Assert.Equal(item.Key, item.Value);
                count++;
            }
            Assert.Equal(100, count);

            Assert.Equal(100, dict.Count);
            Assert.Equal(256, dict.Capacity);
        }

        [Fact]
        public void ConsecutiveInsertionsAndGrow()
        {
            var dict = new FastDictionary<int, int>(4);

            for (int i = 0; i < 100; i++)
                dict.Add(i, i);

            for (int i = 0; i < 100; i++)
            {
                Assert.True(dict.Contains(i));
                Assert.Equal(i, dict[i]);
            }

            Assert.Equal(100, dict.Count);
            Assert.Equal(256, dict.Capacity);
        }

        [Fact]
        public void ConsecutiveRemovesWithoutGrow()
        {
            var dict = new FastDictionary<int, int>(200);

            for (int i = 0; i < 100; i++)
                dict[i] = i;

            for (int i = 0; i < 100; i += 2)
                Assert.True(dict.Remove(i));

            for (int i = 0; i < 100; i++)
            {
                if (i % 2 == 0)
                    Assert.False(dict.Contains(i));
                else
                    Assert.True(dict.Contains(i));
            }

            Assert.Equal(50, dict.Count);
            Assert.Equal(256, dict.Capacity);
        }

        [Fact]
        public void ConsecutiveRemovesWithGrow()
        {
            var dict = new FastDictionary<int, int>(4);

            for (int i = 0; i < 100; i++)
                dict[i] = i;

            for (int i = 0; i < 100; i += 2)
                Assert.True(dict.Remove(i));

            for (int i = 0; i < 100; i++)
            {
                if (i % 2 == 0)
                    Assert.False(dict.Contains(i));
                else
                    Assert.True(dict.Contains(i));
            }

            Assert.Equal(50, dict.Count);
            Assert.Equal(256, dict.Capacity);
        }

        [Fact]
        public void ConsecutiveInsertsWithShrink()
        {
            var dict = new FastDictionary<int, int>();

            for (int i = 0; i < 100; i++)
                dict[i] = i;

            dict.Clear();

            for (int i = 0; i < 33; i++)
                dict[i] = i;

            dict.Remove(32);

            int value;
            Assert.True(dict.TryGetValue(0, out value));
            
            Assert.Equal(32, dict.Count);
            Assert.True(dict.Capacity > 32);
        }

        [Fact]
        public void InsertDeleted()
        {
            var dict = new FastDictionary<int, int>(16);

            dict[1] = 1;
            dict[2] = 2;

            dict.Remove(1);

            dict[17] = 17;

            Assert.False(dict.Contains(1));
            Assert.True(dict.Contains(2));
            Assert.True(dict.Contains(17));

            Assert.Equal(2, dict.Count);
            Assert.Equal(16, dict.Capacity);
        }

        [Fact]
        public void AddDeleted()
        {
            var dict = new FastDictionary<int, int>(16);

            dict.Add(1, 1);
            dict.Add(2, 2);
            dict.Remove(1);
            dict.Add(17, 17);

            Assert.False(dict.Contains(1));
            Assert.True(dict.Contains(2));
            Assert.True(dict.Contains(17));

            Assert.Equal(2, dict.Count);
            Assert.Equal(16, dict.Capacity);
        }

        [Fact]
        public void Duplicates()
        {
            var dict = new FastDictionary<int, int>(16);
            dict[1] = 1;
            dict[1] = 2;

            Assert.Equal(2, dict[1]);
            Assert.Throws<ArgumentException>(() => dict.Add(1, 3));
        }


        [Fact]
        public void EnumeratorsWithJumps()
        {
            var dict = new FastDictionary<int, int>(16);
            dict[1] = 1;
            dict[2] = 2;
            dict[15] = 15;

            int count = 0;
            foreach (var item in dict.Keys)
                count++;
            Assert.Equal(3, count);

            count = 0;
            foreach (var item in dict.Values)
                count++;
            Assert.Equal(3, count);

            count = 0;
            foreach (var item in dict)
                count++;
            Assert.Equal(3, count);
        }      

        [Fact]
        public void Clear()
        {
            var dict = new FastDictionary<int, int>(200);
            for (int i = 0; i < 100; i++)
                dict[i] = i;

            dict.Clear();

            Assert.Equal(0, dict.Count);
            Assert.Equal(256, dict.Capacity);

            for (int i = 0; i < 100; i++)
                Assert.False(dict.Contains(i));
        }

        [Fact]
        public void InsertionAfterClear()
        {
            var dict = new FastDictionary<int, int>(200);
            for (int i = 0; i < 100; i++)
                dict[i] = i;

            dict.Clear();

            Assert.Equal(0, dict.Count);
            Assert.Equal(256, dict.Capacity);

            for (int i = 0; i < 100; i += 10)
                dict[i] = i;


            for (int i = 0; i < 100; i++)
            {
                if (i % 10 == 0)
                    Assert.True(dict.Contains(i));
                else
                    Assert.False(dict.Contains(i));
            }
        }

        [Fact]
        public void KeysArePresent()
        {
            var dict = new FastDictionary<int, int>(4);
            for (int i = 0; i < 100; i++)
                dict[i] = i;

            int count = 0;
            foreach (var key in dict.Keys.ToList())
            {
                Assert.True(dict.ContainsKey(key));
                Assert.Equal(key, dict[key]);
                count++;
            }
            Assert.Equal(100, count);
        }

        [Fact]
        public void ValuesArePresent()
        {
            var dict = new FastDictionary<int, int>(4);
            for (int i = 0; i < 100; i++)
                dict[i] = i;

            int count = 0;
            foreach (var value in dict.Values.ToList())
            {
                Assert.True(dict.ContainsValue(value));
                count++;
            }
            Assert.Equal(100, count);
        }


        private class ForceOutOfRangeHashesEqualityComparer : EqualityComparer<int>
        {
            public override bool Equals(int x, int y)
            {
                return x == y;
            }

            public override int GetHashCode(int obj)
            {
                unchecked
                {
                    if (obj % 2 == 0)
                        return (int)0xFFFFFFFF;
                    else
                        return (int)0xFFFFFFFE;
                }
            }
        }

        [Fact]
        public void UseOfOfBoundsHashes()
        {
            var dict = new FastDictionary<int, int>(16, new ForceOutOfRangeHashesEqualityComparer());
            dict[1] = 1;
            dict[2] = 2;

            Assert.Equal(1, dict[1]);
            Assert.Equal(2, dict[2]);

            dict.Remove(1);
            Assert.False(dict.Contains(1));
            Assert.True(dict.Contains(2));

            dict.Remove(2);
            Assert.False(dict.Contains(1));
            Assert.False(dict.Contains(2));
        }

        [Fact]
        public void InsertAndRemoveWithoutGrowth()
        {
            var dict = new FastDictionary<long, int>(8);

            for (int i = 0; i < 100; i++)
            {
                dict[i] = i;
                dict.Remove(i);
                int dummy;
                Assert.False(dict.TryGetValue(i, out dummy));
            }

            Assert.Equal(0, dict.Count);
            Assert.Equal(8, dict.Capacity);
        }

        [Fact]
        public void AddAndRemoveWithoutGrowth()
        {
            var dict = new FastDictionary<long, int>(8);

            for (int i = 0; i < 100; i++)
            {
                dict.Add(i, i);
                dict.Remove(i);
                int dummy;
                Assert.False(dict.TryGetValue(i, out dummy));
            }

            Assert.Equal(0, dict.Count);
            Assert.Equal(8, dict.Capacity);
        }

        private class A
        {
            private readonly int _hash;

            public A(int hash = 1)
            {
                _hash = hash;
            }

            public override int GetHashCode()
            {
                return _hash;
            }

            public override bool Equals(object obj)
            {
                return ReferenceEquals(obj, this);
            }

            public override string ToString()
            {
                return _hash.ToString();
            }
        }

        [Fact]
        public void ConflictingRemoval()
        {
            var dict = new FastDictionary<A, long>();

            var first = new A();
            var second = new A();

            dict[first] = 1;
            Assert.Equal(1, dict.Count);
            dict[second] = 2;
            Assert.Equal(2, dict.Count);
            dict.Remove(first);
            Assert.Equal(1, dict.Count);
            Assert.Throws<ArgumentException>(() => dict.Add(second, 3));

            Assert.Equal(1, dict.Count);
            Assert.Equal(1, dict.Keys.Distinct().Count());
        }

        [Fact]
        public void ConflictingRemoval2()
        {
            var dict = new FastDictionary<A, long>();

            var first = new A();
            var second = new A();

            dict[first] = 1;
            Assert.Equal(1, dict.Count);
            dict[second] = 2;
            Assert.Equal(2, dict.Count);
            dict.Remove(first);
            Assert.Equal(1, dict.Count);
            dict[second] = 3;
            Assert.Equal(1, dict.Count);
            Assert.Equal(1, dict.Keys.Distinct().Count());
        }

        [Fact]
        public void ConflictingAddTwice()
        {
            var dict = new FastDictionary<A, long>();

            var first = new A(1);
            var second = new A(33);
            var third = new A(66);

            dict[first] = 1;
            Assert.Equal(1, dict.Count);
            dict[second] = 2;
            Assert.Equal(2, dict.Count);
            dict[third] = 3;
            Assert.Equal(3, dict.Count);
            dict.Remove(first);
            Assert.Equal(2, dict.Count);
            Assert.Throws<ArgumentException>(() => dict.Add(third, 4));

            Assert.Equal(2, dict.Count);
            Assert.Equal(2, dict.Keys.Distinct().Count());
        }

        [Fact]
        public void ConflictingSetTwice()
        {
            var dict = new FastDictionary<A, long>();

            var first = new A(1);
            var second = new A(33);
            var third = new A(66);

            dict[first] = 1;
            Assert.Equal(1, dict.Count);
            dict[second] = 2;
            Assert.Equal(2, dict.Count);
            dict[third] = 3;
            Assert.Equal(3, dict.Count);
            dict.Remove(first);
            Assert.Equal(2, dict.Count);
            dict[third] = 4;

            Assert.Equal(2, dict.Count);
            Assert.Equal(2, dict.Keys.Distinct().Count());
        }

        public void RandomTest(int i)
        {
            var rng = new Random(i);

            var dict = new Dictionary<int, int>(32, EqualityComparer<int>.Default);
            var fdict = new FastDictionary<int, int>(31, EqualityComparer<int>.Default);
            for (int j = 0; j < 1000; j++)
            {
                int rnd = rng.Next();
                dict[rnd] = rnd;
                fdict[rnd] = rnd;
            }

            Assert.Equal(dict.Count, fdict.Count);
            Assert.Equal(dict.Keys.Count, fdict.Keys.Count);
            Assert.Equal(dict.Values.Count, fdict.Values.Count);

            foreach (var item in dict)
            {
                Assert.Equal(item.Value, fdict[item.Key]);
            }

            var deleted = new List<int>();
            var notDeleted = new List<int>();

            int count = dict.Count;
            foreach (var item in dict)
            {
                int rnd = rng.Next();
                if (rnd % 2 == 0)
                {
                    Assert.True(fdict.Remove(item.Key));
                    deleted.Add(item.Key);

                    count--;
                }
                else
                {
                    notDeleted.Add(item.Key);
                }

                Assert.Equal(count, fdict.Count);
            }

            Assert.Equal(notDeleted.Count, fdict.Count);
            Assert.Equal(notDeleted.Count, fdict.Keys.Count);
            Assert.Equal(notDeleted.Count, fdict.Values.Count);

            foreach (var item in deleted)
            {
                Assert.False(fdict.Contains(item));
            }

            foreach (var item in notDeleted)
            {
                Assert.True(fdict.Contains(item));
            }


            fdict.Clear();

            foreach (var item in dict)
            {
                Assert.False(fdict.Contains(item.Key));
            }

            Assert.Equal(0, fdict.Count);
            Assert.Equal(0, fdict.Keys.Count);
            Assert.Equal(0, fdict.Values.Count);
        }

        [Fact]
        public void Random()
        {
            for (int i = 0; i < 1000; i++)
            {
                RandomTest(i);
            }
        }

    }
}
