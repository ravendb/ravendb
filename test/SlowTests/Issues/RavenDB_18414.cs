using System;
using System.Collections.Generic;
using FastTests;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList;

public class RavenDB_18414 : RavenTestBase
{
    public RavenDB_18414(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void WillNotAddTypeToNestedObjectsOfSameClass()
    {
        using var store = GetDocumentStore();


        using (var session = store.OpenSession())
        {
            var test = new TestingEntity()
            {
                MyValues = new Dictionary<string, string> { { "", "" } },
                MyCollection =
                {
                    new TestingItemEntity { MyName = "bbbb" } 
                }
            };


            session.Store(test, "items/test");

            session.SaveChanges();

        }

        using (var session = store.OpenSession())
        {
            var newCollection = new List<TestingItemEntity> { new TestingItemEntity { MyName = "abba" } };


            session.Advanced.Patch<TestingEntity, ICollection<TestingItemEntity>>("items/test", x => x.MyCollection, newCollection);

            session.SaveChanges();
        }
        
        using (var session = store.OpenSession())
        {
            var obj = session.Load<BlittableJsonReaderObject>("items/test");
            Assert.DoesNotContain("$type", obj.ToString());
        }
    }

    private abstract class BaseStorageEntity

    {
        public string Id { get; set; }

        public Guid CreatedBy { get; set; }

        public Guid ModifiedBy { get; set; }

        public DateTime CreatedDate { get; set; } = DateTime.UtcNow;

        public DateTime ModifiedDate { get; set; } = DateTime.UtcNow;

        public bool IsDeactivated { get; set; }

        public DateTime? DeactivatedDate { get; set; }
    }


    private interface IMyRepo

    {
        void Test();
    }

    private class TestingEntity : BaseStorageEntity

    {
        public Dictionary<string, string> MyValues { get; set; }

        public ICollection<TestingItemEntity> MyCollection { get; set; } = new List<TestingItemEntity>();
    }


    private class TestingItemEntity

    {
        public string MyName { get; set; }
    }
}
