using Raven.Abstractions.Linq;
using Raven.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Raven.Imports.Newtonsoft.Json;
namespace Raven.Tests.Issues
{
    public class RavenDb_2123 : RavenTestBase
    {
        private class SomeChildType
        {
        }

        private class Parent
        {
            public Guid Id { get; set; }
            [JsonProperty(TypeNameHandling = TypeNameHandling.All)]
            public object Children { get; set; }
        }
        private class ParentWithoutSharedMetadata
        {
            public Guid Id { get; set; }
            [JsonProperty(TypeNameHandling = TypeNameHandling.All)]
            public object ChildrenWithoutSharedMetadata { get; set; }
        }

        // Fails
        [Fact]
        public void CanRestoreGrandchildrenWithDifferentParentClass()
        {
            var parentId = Guid.NewGuid();
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Parent
                    {
                        Id = parentId,
                        Children = new List<ParentWithoutSharedMetadata>
                        {
                            new ParentWithoutSharedMetadata
                            {
                                Id = Guid.NewGuid(),
                                ChildrenWithoutSharedMetadata = new List<SomeChildType>(){new SomeChildType()}
                            }
                        }
                    });
                    session.SaveChanges();
                }
               
                WaitForUserToContinueTheTest(store);
                   
                using (var session = store.OpenSession())
                {
                    var grandParent = session.Load<Parent>(parentId);
                    var parent = (grandParent.Children as List<ParentWithoutSharedMetadata>).Single();
                    Assert.IsAssignableFrom<List<SomeChildType>>(parent.ChildrenWithoutSharedMetadata);

                    /*
                     Exception
                        Xunit.Sdk.IsAssignableFromException: Assert.IsAssignableFrom() Failure
                        Expected: System.Collections.Generic.List`1[RavenSerializationBugTests.RavenTests.JsonAttributesRespectedOnNestedClasses+SomeChildType]
                        Actual:   Raven.Abstractions.Linq.DynamicList
                           at RavenSerializationBugTests.RavenTests.JsonAttributesRespectedOnNestedClasses.CanRestoreGrandchildrenWithDifferentParentClass() in D:\OtherDev\GitHub\NEventStore\src\RavenSerializationBugTests\RavenTests\JsonAttributesRespectedOnNestedClasses.cs:line 59

                     */

                    var grandchildren = (parent.ChildrenWithoutSharedMetadata as List<SomeChildType>);
                    Assert.Equal(1, grandchildren.Count);
                }
            }
        }

        // Succeeds
        [Fact]
        public void CanRestoreGrandchildrenWithDifferentParentClassIfIncludingAllTypeNames()
        {
            var parentId = Guid.NewGuid();
            using (var store = NewDocumentStore())
            {
                store.Conventions.CustomizeJsonSerializer = s => s.TypeNameHandling = TypeNameHandling.All;
                using (var session = store.OpenSession())
                {
                    session.Store(new Parent
                    {
                        Id = parentId,
                        Children = new List<ParentWithoutSharedMetadata>
                        {
                            new ParentWithoutSharedMetadata
                            {
                                Id = Guid.NewGuid(),
                                ChildrenWithoutSharedMetadata = new List<SomeChildType>(){new SomeChildType()}
                            }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var grandParent = session.Load<Parent>(parentId);
                    var parent = (grandParent.Children as List<ParentWithoutSharedMetadata>).Single();
                    var grandchildren = (parent.ChildrenWithoutSharedMetadata as List<SomeChildType>);
                    Assert.Equal(1, grandchildren.Count);
                }
            }
        }

        // Succeeds
        [Fact]
        public void CanRestoreGrandchildrenWhenParentSharesCachedMetadata()
        {
            var parentId = Guid.NewGuid();
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Parent
                    {
                        Id = parentId,
                        Children = new List<Parent>
                        {
                            new Parent
                            {
                                Id = Guid.NewGuid(),
                                Children = new List<SomeChildType>(){new SomeChildType()}
                            }
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var grandParent = session.Load<Parent>(parentId);
                    var parent = (grandParent.Children as List<Parent>).Single();
                    var grandchildren = (parent.Children as List<SomeChildType>);
                    Assert.Equal(1, grandchildren.Count);
                }
            }
        }
    }
}
