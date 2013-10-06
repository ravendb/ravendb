using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Trees
{
	public unsafe class Iteration : StorageTest
	{
		[Fact]
		public void EmptyIterator()
		{
			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var iterator = Env.Root.Iterate(tx);
				Assert.False(iterator.Seek(Slice.BeforeAllKeys));
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var iterator = Env.Root.Iterate(tx);
				Assert.False(iterator.Seek(Slice.AfterAllKeys));
			}
		}

		[Fact]
		public void CanIterateInOrder()
		{
			var random = new Random();
			var buffer = new byte[512];
			random.NextBytes(buffer);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 25; i++)
				{
					Env.Root.Add(tx, i.ToString("0000"), new MemoryStream(buffer));
				}

				tx.Commit();
			}

			using (var tx = Env.NewTransaction(TransactionFlags.Read))
			{
				var iterator = Env.Root.Iterate(tx);
				Assert.True(iterator.Seek(Slice.BeforeAllKeys));

				var slice = new Slice(SliceOptions.Key);
				for (int i = 0; i < 24; i++)
				{
					slice.Set(iterator.Current);

					Assert.Equal(i.ToString("0000"), slice);

					Assert.True(iterator.MoveNext());
				}

				slice.Set(iterator.Current);

				Assert.Equal(24.ToString("0000"), slice);
				Assert.False(iterator.MoveNext());
			}
		}

        [Fact(Skip = "Not supported currently")]
        public void Iterator_ForwardIteration_TheSameKey_In_WriteBatch_And_Tree()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            const string BEFORE_UPDATE_VALUE = "foo";
            const string AFTER_UPDATE_VALUE = "updated foo";

            //first add without transactions
            using (var value = new MemoryStream(buffer))
            {
                using (var test2EntryValue = StreamFor(BEFORE_UPDATE_VALUE))
                {
                    using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        Env.CreateTree(tx, "tree");
                        Env.GetTree(tx, "tree").Add(tx, "Test1", value);
                        Env.GetTree(tx, "tree").Add(tx, "Test2", test2EntryValue);

                        tx.Commit();
                    }
                }
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Test2", StreamFor(AFTER_UPDATE_VALUE), "tree");
                    writeBatch.Add("Test3", new MemoryStream(buffer), "tree");
                    var foundKeys = new Dictionary<string,Stream>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek(Slice.BeforeAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString(), iter.CreateStreamForCurrent());
                        } while (iter.MoveNext());
                    }

                    Assert.Equal(new List<string> { "Test1", "Test2", "Test3" }, foundKeys.Keys.ToList(), StringComparer.InvariantCulture);
                    var fetchedTest2Value = Encoding.UTF8.GetString(foundKeys["Test2"].ReadData());
                    Assert.Equal(fetchedTest2Value, AFTER_UPDATE_VALUE);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_BackwardIteration_TheSameKey_In_WriteBatch_And_Tree()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            const string BEFORE_UPDATE_VALUE = "foo";
            const string AFTER_UPDATE_VALUE = "updated foo";

            //first add without transactions
            using (var value = new MemoryStream(buffer))
            {
                using (var test2EntryValue = StreamFor(BEFORE_UPDATE_VALUE))
                {
                    using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        Env.CreateTree(tx, "tree");
                        Env.GetTree(tx, "tree").Add(tx, "Test1", value);
                        Env.GetTree(tx, "tree").Add(tx, "Test2", test2EntryValue);

                        tx.Commit();
                    }
                }
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Test2", StreamFor(AFTER_UPDATE_VALUE), "tree");
                    writeBatch.Add("Test3", new MemoryStream(buffer), "tree");
                    var foundKeys = new Dictionary<string, Stream>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek(Slice.AfterAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString(), iter.CreateStreamForCurrent());
                        } while (iter.MovePrev());
                    }

                    Assert.Equal(new List<string> { "Test3", "Test2", "Test1" }, foundKeys.Keys.ToList(), StringComparer.InvariantCulture);
                    var fetchedTest2Value = Encoding.UTF8.GetString(foundKeys["Test2"].ReadData());
                    Assert.Equal(fetchedTest2Value, AFTER_UPDATE_VALUE);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
	    public void Iterator_ForwardIteration_With_WriteBatch()
	    {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
	        using(var memoryStream = new MemoryStream(buffer))
	        using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
	        {
	            Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Test1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test2", memoryStream);

                tx.Commit();
            }

	        using (var snapshot = Env.CreateSnapshot())
	        {
	            using (var writeBatch = new WriteBatch())
	            {
                    writeBatch.Add("Test3", new MemoryStream(buffer),"tree");
	                var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree",writeBatch))
                    {
                        iter.Seek(Slice.BeforeAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MoveNext());
                    }

                    Assert.Equal(new List<string> { "Test1", "Test2", "Test3" }, foundKeys, StringComparer.InvariantCulture);
	            }
	        }
	    }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_ForwardIteration_With_WriteBatch_SkipDeletedValues_ThatAreInTree()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Test1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test3", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Test4", new MemoryStream(buffer), "tree");
                    writeBatch.Delete("Test2", "tree");

                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek(Slice.BeforeAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MoveNext());
                    }

                    Assert.Equal(new List<string> { "Test1", "Test3", "Test4" }, foundKeys, StringComparer.InvariantCulture);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_BackwardIteration_With_WriteBatch_SkipDeletedValues_ThatAreInTree()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Test1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test3", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Test4", new MemoryStream(buffer), "tree");
                    writeBatch.Delete("Test2", "tree");

                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek(Slice.AfterAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MovePrev());
                    }

                    Assert.Equal(new List<string> { "Test4", "Test3", "Test1" }, foundKeys, StringComparer.InvariantCulture);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_ForwardIteration_With_WriteBatch_And_SeekIntoTreeIterator()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Test1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test3", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test4", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Test5", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test6", new MemoryStream(buffer), "tree");
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek("Test3");
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MoveNext());
                    }

                    Assert.Equal(new List<string> { "Test3", "Test4", "Test5", "Test6" },foundKeys, StringComparer.InvariantCulture);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_BackwardIteration_With_WriteBatch_And_SeekIntoTreeIterator()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Test1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test3", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test4", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Test5", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test6", new MemoryStream(buffer), "tree");
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek("Test3");
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MovePrev());
                    }

                    Assert.Equal(new List<string> { "Test3", "Test2", "Test1" }, foundKeys, StringComparer.InvariantCulture);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_BackwardIteration_With_WriteBatch_And_SeekIntoAddedValues()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Test1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test8", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Test3", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test4", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test5", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test6", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test7", new MemoryStream(buffer), "tree");
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek("Test5");
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MovePrev());
                    }

                    Assert.Equal(new List<string> { "Test5", "Test4", "Test3", "Test8", "Test2", "Test1" }, foundKeys, StringComparer.InvariantCulture);
                }
            }
        }


        [Fact(Skip = "Not supported currently")]
        public void Iterator_ForwardIteration_With_WriteBatch_And_SeekIntoAddedValues()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Test1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test8", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Test3", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test4", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test5", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test6", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Test7", new MemoryStream(buffer), "tree");
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek("Test4");
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MoveNext());
                    }

                    Assert.Equal(new List<string> { "Test4", "Test5", "Test6", "Test7" }, foundKeys, StringComparer.InvariantCulture);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_BackwardIteration_With_WriteBatch()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Test1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Test2", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Test3", new MemoryStream(buffer), "tree");
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek(Slice.AfterAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MovePrev());
                    }

                    Assert.Equal(new List<string> { "Test3", "Test2", "Test1" },foundKeys, StringComparer.InvariantCulture);
                }
            }
        }


        [Fact(Skip = "Not supported currently")]
	    public void Iterator_ForwardIteration_With_WriteBatch_And_RequiredPrefix()
	    {
	        var random = new Random();
	        var buffer = new byte[512];
	        random.NextBytes(buffer);

	        //first add without transactions
	        using (var memoryStream = new MemoryStream(buffer))
	        using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
	        {
	            Env.CreateTree(tx, "tree");
	            Env.GetTree(tx, "tree").Add(tx, "Foo1", memoryStream);
	            Env.GetTree(tx, "tree").Add(tx, "Bar1", memoryStream);
	            Env.GetTree(tx, "tree").Add(tx, "Foo2", memoryStream);
	            Env.GetTree(tx, "tree").Add(tx, "Bar2", memoryStream);
	            Env.GetTree(tx, "tree").Add(tx, "Foo3", memoryStream);
	            Env.GetTree(tx, "tree").Add(tx, "Bar3", memoryStream);

	            tx.Commit();
	        }

	        using (var snapshot = Env.CreateSnapshot())
	        {
	            using (var writeBatch = new WriteBatch())
	            {
	                writeBatch.Add("Foo4", new MemoryStream(buffer), "tree");
	                writeBatch.Add("Bar4", new MemoryStream(buffer), "tree");
	                writeBatch.Add("Foo5", new MemoryStream(buffer), "tree");
	                writeBatch.Add("Bar5", new MemoryStream(buffer), "tree");
	                writeBatch.Add("Foo6", new MemoryStream(buffer), "tree");
	                writeBatch.Add("Bar6", new MemoryStream(buffer), "tree");
	                var foundKeys = new List<string>();
	                using (var iter = snapshot.Iterate("tree", writeBatch))
	                {
	                    iter.RequiredPrefix = "Bar";
	                    iter.Seek(Slice.BeforeAllKeys);
	                    do
	                    {
	                        foundKeys.Add(iter.CurrentKey.ToString());
	                    } while (iter.MoveNext());
	                }

	                Assert.Equal(new List<string> {"Bar1", "Bar2", "Bar3", "Bar4", "Bar5", "Bar6"}, foundKeys,
	                    StringComparer.InvariantCulture);
	            }
	        }
	    }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_ForwardIteration_With_WriteBatch_And_MaxKey_InTreeIterator()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Bar1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar3", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar4", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Bar5", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Bar6", new MemoryStream(buffer), "tree");
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.MaxKey = "Bar3";
                        iter.Seek(Slice.BeforeAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MoveNext());
                    }

                    Assert.Equal(new List<string> { "Bar1", "Bar2"}, foundKeys,
                        StringComparer.InvariantCulture);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_ForwardIteration_With_WriteBatch_And_MaxKey_InWriteBatch()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Bar1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar2", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Bar3", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Bar4", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Bar5", new MemoryStream(buffer), "tree");
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.MaxKey = "Bar4";
                        iter.Seek(Slice.BeforeAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MoveNext());
                    }

                    Assert.Equal(new List<string> { "Bar1", "Bar2", "Bar3" }, foundKeys,
                        StringComparer.InvariantCulture);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_ForwardIteration_With_WriteBatch_And_EmptyAddedValues()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Bar1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar3", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar4", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek(Slice.BeforeAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MoveNext());
                    }

                    Assert.Equal(new List<string> { "Bar1", "Bar2", "Bar3", "Bar4" }, foundKeys,
                        StringComparer.InvariantCulture);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_BackwardIteration_With_WriteBatch_And_EmptyAddedValues()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Bar1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar3", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar4", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.Seek(Slice.AfterAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MovePrev());
                    }

                    Assert.Equal(new List<string> { "Bar4", "Bar3", "Bar2", "Bar1" }, foundKeys,
                        StringComparer.InvariantCulture);
                }
            }
        }

        [Fact(Skip = "Not supported currently")]
        public void Iterator_BackwardIteration_With_WriteBatch_And_RequiredPrefix()
        {
            var random = new Random();
            var buffer = new byte[512];
            random.NextBytes(buffer);

            //first add without transactions
            using (var memoryStream = new MemoryStream(buffer))
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.CreateTree(tx, "tree");
                Env.GetTree(tx, "tree").Add(tx, "Foo1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar1", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Foo2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar2", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Foo3", memoryStream);
                Env.GetTree(tx, "tree").Add(tx, "Bar3", memoryStream);

                tx.Commit();
            }

            using (var snapshot = Env.CreateSnapshot())
            {
                using (var writeBatch = new WriteBatch())
                {
                    writeBatch.Add("Foo4", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Bar4", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Foo5", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Bar5", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Foo6", new MemoryStream(buffer), "tree");
                    writeBatch.Add("Bar6", new MemoryStream(buffer), "tree");
                    var foundKeys = new List<string>();
                    using (var iter = snapshot.Iterate("tree", writeBatch))
                    {
                        iter.RequiredPrefix = "Bar";
                        iter.Seek(Slice.AfterAllKeys);
                        do
                        {
                            foundKeys.Add(iter.CurrentKey.ToString());
                        } while (iter.MovePrev());
                    }

                    Assert.Equal(new List<string> { "Bar6", "Bar5", "Bar4", "Bar3", "Bar2", "Bar1" }, foundKeys, StringComparer.InvariantCulture);
                }
            }
        }

	}
}