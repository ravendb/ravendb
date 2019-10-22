// -----------------------------------------------------------------------
//  <copyright file="ProjectionTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class ProjectionTests : RavenTestBase
    {
        public ProjectionTests(ITestOutputHelper output) : base(output)
        {
        }

        private void CreateData(IDocumentSession session)
        {
            var list = new List<Foo>
            {
                new Foo {Data = 1},
                new Foo {Data = 2},
                new Foo {Data = 3},
                new Foo {Data = 4},
            };

            list.ForEach(foo => session.Store(foo));
            session.SaveChanges();
        }

        //This works as expected
        [Fact]
        public void ActuallyGetData()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                CreateData(session);

                var foos = session.Query<Foo>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(foo => foo.Data > 1)
                    .Select(foo => new FooWithId
                    {
                        Id = foo.Id,
                        Data = foo.Data
                    })
                    .ToList();

                Assert.True(foos.Count == 3);
            }
        }

        //This works as expected
        [Fact]
        public void ShouldBeAbleToProjectIdOntoAnotherFieldCalledId()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                CreateData(session);

                var foos = session.Query<Foo>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(foo => foo.Data > 1)
                    .Select(foo => new FooWithId
                    {
                        Id = foo.Id,
                        Data = foo.Data
                    })
                    .ToList();

                Assert.NotNull(foos[0].Id);
            }
        }

        //Fails
        [Fact]
        public void ShouldBeAbleToProjectIdOntoAnotherName()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                CreateData(session);

                var foos = session.Query<Foo>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(foo => foo.Data > 1)
                    .Select(foo => new FooWithFooId
                    {
                        FooId = foo.Id,
                        Data = foo.Data
                    })
                    .ToList();

                Assert.NotNull(foos[0].FooId);
            }
        }

        [Fact]
        public void ShouldBeAbleToProjectIdOntoAnotherName_ButIdFieldWillBeFilledAnyway()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                CreateData(session);

                var foos = session.Query<Foo>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(foo => foo.Data > 1)
                    .Select(foo => new FooWithFooIdAndId
                    {
                        FooId = foo.Id,
                        Data2 = foo.Data
                    })
                    .ToList();

                Assert.NotNull(foos[0].Id);
                Assert.NotNull(foos[0].FooId);
                Assert.Equal(foos[0].Id, foos[0].FooId);
            }
        }

        private class Foo
        {
            public string Id { set; get; }
            public int Data { set; get; }
        }

        private class FooWithFooId
        {
            public string FooId { set; get; }
            public int Data { set; get; }
        }

        private class FooWithId
        {
            public string Id { set; get; }
            public int Data { set; get; }
        }

        private class FooWithFooIdAndId
        {
            public string FooId { set; get; }
            public string Id { set; get; }
            public int Data2 { set; get; }
        }
    }
}
