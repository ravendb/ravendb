﻿using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14439 : RavenTestBase
    {
        public RavenDB_14439(ITestOutputHelper output) : base(output)
        {
        }

        public class Item
        {
            public string Name;
            public string ChangeVectorVal;
        }

        [Fact]
        public void CanGetChangeVectorForPatching()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Item
                    {
                        Name = "Oren"
                    },"users/oren");
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    s.Advanced.Defer(new PatchCommandData("users/oren", null, new PatchRequest
                    {
                        Script = @"this.ChangeVectorVal = getMetadata(this)['@change-vector'];"
                    }, null));
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    Item load = s.Load<Item>("users/oren");
                    Assert.NotNull(load.ChangeVectorVal);
                }
            }
        }
    }
}
