using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12584 : RavenTestBase
    {
        [Fact]
        public void DynamicArrayContainsMethodShouldReturnCorrectValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(_user);
                    session.SaveChanges();

                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexArraysContainsAndIndexOf",
                        Maps =
                        {
                            @"from user in docs.ArraysUsers
                                select new {
                                            ContainsOfMyIntArray = user.MyIntArray.Contains(2),
                                            ContainsOfMyShortArray = user.MyShortArray.Contains(2),
                                            ContainsOfMyDoubleArray = user.MyDoubleArray.Contains(0.2),
                                            ContainsOfMyFloatArray = user.MyFloatArray.Contains(2.3f),
                                            ContainsOfMyStringArray = user.MyStringArray.Contains(""stu""),
                                            ContainsOfMyCharArray = user.MyCharArray.Contains('d'),
                                            }"
                        },
                        Type = IndexType.Map
                    }));
                    WaitForIndexing(store);

                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.ContainsOfMyIntArray).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.ContainsOfMyShortArray).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.ContainsOfMyDoubleArray).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.ContainsOfMyFloatArray).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.ContainsOfMyStringArray).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.ContainsOfMyCharArray).OfType<ArraysUser>().ToArray().Length);
                }
            }
        }

        [Fact]
        public void DynamicArrayIndexOfMethodShouldReturnCorrectValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(_user);
                    session.SaveChanges();

                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexArraysContainsAndIndexOf",
                        Maps =
                        {
                            @"from user in docs.ArraysUsers
                                select new {
                                            IndexOfOfMyIntArray = user.MyIntArray.IndexOf(3),
                                            IndexOfOfMyShortArray = user.MyShortArray.IndexOf(1),
                                            IndexOfOfMyDoubleArray = user.MyDoubleArray.IndexOf(7.8),
                                            IndexOfOfMyFloatArray = user.MyFloatArray.IndexOf(9.10f),
                                            IndexOfOfMyStringArray = user.MyStringArray.IndexOf(""ghi""),
                                            IndexOfOfMyCharArray = user.MyCharArray.IndexOf('j'),

                                            IndexOfOfMyIntArrayWithIndex = user.MyIntArray.IndexOf(2, 3),
                                            IndexOfOfMyShortArrayWithIndex = user.MyShortArray.IndexOf(7, 3),
                                            IndexOfOfMyDoubleArrayWithIndex = user.MyDoubleArray.IndexOf(9.10, 3),
                                            IndexOfOfMyFloatArrayWithIndex = user.MyFloatArray.IndexOf(5.6f, 3),
                                            IndexOfOfMyStringArrayWithIndex = user.MyStringArray.IndexOf(""xyz"", 1),
                                            IndexOfOfMyCharArrayWithIndex = user.MyCharArray.IndexOf('g', 0),

                                            IndexOfOfMyIntArrayWithIndexWithCount = user.MyIntArray.IndexOf(2, 1, 7),
                                            IndexOfOfMyShortArrayWithIndexWithCount = user.MyShortArray.IndexOf(10, 0, 11),
                                            IndexOfOfMyDoubleArrayWithIndexWithCount = user.MyDoubleArray.IndexOf(0.2, 0, 7),
                                            IndexOfOfMyFloatArrayWithIndexWithCount = user.MyFloatArray.IndexOf(7.8f, 5, 6),
                                            IndexOfOfMyStringArrayWithIndexWithCount = user.MyStringArray.IndexOf(""vw"", 6, 3),
                                            IndexOfOfMyCharArrayWithIndexWithCount = user.MyCharArray.IndexOf('x', 1, 8),
                                            }"
                        },
                        Type = IndexType.Map
                    }));
                    WaitForIndexing(store);

                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyIntArray == 3).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyShortArray == 1).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyDoubleArray == 7).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyFloatArray == 9).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyStringArray == 2).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyCharArray == 9).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyIntArrayWithIndex == -1).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyShortArrayWithIndex == 7).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyDoubleArrayWithIndex == 9).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyFloatArrayWithIndex == 5).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyStringArrayWithIndex == 8).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyCharArrayWithIndex == 6).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyIntArrayWithIndexWithCount == 2).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyShortArrayWithIndexWithCount == 10).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyDoubleArrayWithIndexWithCount == 0).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyFloatArrayWithIndexWithCount == 7).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyStringArrayWithIndexWithCount == 7).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysContainsAndIndexOf").Where(x => x.IndexOfOfMyCharArrayWithIndexWithCount == -1).OfType<ArraysUser>().ToArray().Length);
                }
            }
        }

        [Fact]
        public void DynamicArrayLastIndexOfMethodShouldReturnCorrectValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(_user);
                    session.SaveChanges();

                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Name = "TestIndexArraysLastIndexOf",
                        Maps =
                        {
                            @"from user in docs.ArraysUsers
                                select new {
                                            LastIndexOfOfMyIntArray = user.MyIntArray.LastIndexOf(3),
                                            LastIndexOfOfMyShortArray = user.MyShortArray.LastIndexOf(1),
                                            LastIndexOfOfMyDoubleArray = user.MyDoubleArray.LastIndexOf(7.8),
                                            LastIndexOfOfMyFloatArray = user.MyFloatArray.LastIndexOf(9.10f),
                                            LastIndexOfOfMyStringArray = user.MyStringArray.LastIndexOf(""ghi""),
                                            LastIndexOfOfMyCharArray = user.MyCharArray.LastIndexOf('j'),

                                            LastIndexOfOfMyIntArrayWithIndex = user.MyIntArray.LastIndexOf(2, 3),
                                            LastIndexOfOfMyShortArrayWithIndex = user.MyShortArray.LastIndexOf(7, 3),
                                            LastIndexOfOfMyDoubleArrayWithIndex = user.MyDoubleArray.LastIndexOf(9.10, 10),
                                            LastIndexOfOfMyFloatArrayWithIndex = user.MyFloatArray.LastIndexOf(5.6f, 5),
                                            LastIndexOfOfMyStringArrayWithIndex = user.MyStringArray.LastIndexOf(""xyz"", 8),
                                            LastIndexOfOfMyCharArrayWithIndex = user.MyCharArray.LastIndexOf('g', 6),

                                            LastIndexOfOfMyIntArrayWithIndexWithCount = user.MyIntArray.LastIndexOf(2, 3, 4),
                                            LastIndexOfOfMyShortArrayWithIndexWithCount = user.MyShortArray.LastIndexOf(10, 10, 11),
                                            LastIndexOfOfMyDoubleArrayWithIndexWithCount = user.MyDoubleArray.LastIndexOf(0.2, 0, 1),
                                            LastIndexOfOfMyFloatArrayWithIndexWithCount = user.MyFloatArray.LastIndexOf(7.8f, 5, 5),
                                            LastIndexOfOfMyStringArrayWithIndexWithCount = user.MyStringArray.LastIndexOf(""vw"", 8, 9),
                                            LastIndexOfOfMyCharArrayWithIndexWithCount = user.MyCharArray.LastIndexOf('x', 9, 10),
                                            }"
                        },
                        Type = IndexType.Map
                    }));
                    WaitForIndexing(store);

                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyIntArray == 3).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyShortArray == 1).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyDoubleArray == 7).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyFloatArray == 9).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyStringArray == 2).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyCharArray == 9).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyIntArrayWithIndex == 2).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyShortArrayWithIndex == -1).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyDoubleArrayWithIndex == 9).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyFloatArrayWithIndex == 5).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyStringArrayWithIndex == 8).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyCharArrayWithIndex == 6).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyIntArrayWithIndexWithCount == 2).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyShortArrayWithIndexWithCount == 10).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyDoubleArrayWithIndexWithCount == 0).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyFloatArrayWithIndexWithCount == -1).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyStringArrayWithIndexWithCount == 7).OfType<ArraysUser>().ToArray().Length);
                    Assert.Equal(1, session.Query<Result>("TestIndexArraysLastIndexOf").Where(x => x.LastIndexOfOfMyCharArrayWithIndexWithCount == -1).OfType<ArraysUser>().ToArray().Length);
                }
            }
        }

        public class ArraysUser
        {
            public Array MyIntArray { get; set; }
            public Array MyShortArray { get; set; }
            public Array MyDoubleArray { get; set; }
            public Array MyFloatArray { get; set; }
            public Array MyStringArray { get; set; }
            public Array MyCharArray { get; set; }
        }

        private readonly ArraysUser _user = new ArraysUser
        {
            MyIntArray = new[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            MyShortArray = new short[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
            MyDoubleArray = new[] {0.2, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9.10, 10.11},
            MyFloatArray = new[] {0.2f, 1.2f, 2.3f, 3.4f, 4.5f, 5.6f, 6.7f, 7.8f, 8.9f, 9.10f, 10.11f},
            MyStringArray = new[] {"abc", "def", "ghi", "jkl", "mno", "pqr", "stu", "vw", "xyz"},
            MyCharArray = new[] {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'}
        };

        public class Result
        {
            public bool ContainsOfMyIntArray { get; set; }
            public bool ContainsOfMyShortArray { get; set; }
            public bool ContainsOfMyDoubleArray { get; set; }
            public bool ContainsOfMyFloatArray { get; set; }
            public bool ContainsOfMyStringArray { get; set; }
            public bool ContainsOfMyCharArray { get; set; }

            public int IndexOfOfMyIntArray { get; set; }
            public int IndexOfOfMyShortArray { get; set; }
            public int IndexOfOfMyDoubleArray { get; set; }
            public int IndexOfOfMyFloatArray { get; set; }
            public int IndexOfOfMyStringArray { get; set; }
            public int IndexOfOfMyCharArray { get; set; }
            public int IndexOfOfMyIntArrayWithIndex { get; set; }
            public int IndexOfOfMyShortArrayWithIndex { get; set; }
            public int IndexOfOfMyDoubleArrayWithIndex { get; set; }
            public int IndexOfOfMyFloatArrayWithIndex { get; set; }
            public int IndexOfOfMyStringArrayWithIndex { get; set; }
            public int IndexOfOfMyCharArrayWithIndex { get; set; }
            public int IndexOfOfMyIntArrayWithIndexWithCount { get; set; }
            public int IndexOfOfMyShortArrayWithIndexWithCount { get; set; }
            public int IndexOfOfMyDoubleArrayWithIndexWithCount { get; set; }
            public int IndexOfOfMyFloatArrayWithIndexWithCount { get; set; }
            public int IndexOfOfMyStringArrayWithIndexWithCount { get; set; }
            public int IndexOfOfMyCharArrayWithIndexWithCount { get; set; }

            public int LastIndexOfOfMyIntArray { get; set; }
            public int LastIndexOfOfMyShortArray { get; set; }
            public int LastIndexOfOfMyDoubleArray { get; set; }
            public int LastIndexOfOfMyFloatArray { get; set; }
            public int LastIndexOfOfMyStringArray { get; set; }
            public int LastIndexOfOfMyCharArray { get; set; }
            public int LastIndexOfOfMyIntArrayWithIndex { get; set; }
            public int LastIndexOfOfMyShortArrayWithIndex { get; set; }
            public int LastIndexOfOfMyDoubleArrayWithIndex { get; set; }
            public int LastIndexOfOfMyFloatArrayWithIndex { get; set; }
            public int LastIndexOfOfMyStringArrayWithIndex { get; set; }
            public int LastIndexOfOfMyCharArrayWithIndex { get; set; }
            public int LastIndexOfOfMyIntArrayWithIndexWithCount { get; set; }
            public int LastIndexOfOfMyShortArrayWithIndexWithCount { get; set; }
            public int LastIndexOfOfMyDoubleArrayWithIndexWithCount { get; set; }
            public int LastIndexOfOfMyFloatArrayWithIndexWithCount { get; set; }
            public int LastIndexOfOfMyStringArrayWithIndexWithCount { get; set; }
            public int LastIndexOfOfMyCharArrayWithIndexWithCount { get; set; }
        }
    }
}
