// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1820.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Runtime.Serialization;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;
using Raven.Abstractions.Json;
using System.Linq;

namespace Raven.Tests.Issues
{
    public class RavenDB_1820 : RavenTest
    {
        private class Nested
        {
            public string Id { get; set; }
            public string Property1 { get; set; }
            public string Property2 { get; set; }
            public Nested Inner { get; set; }
            public List<Nested> InnerList { get; set; } 
        }
        

        [Fact]
        public void SelectingObjectsShouldReturnSnapshots()
        {
            var sourceObject =
                RavenJObject.FromObject(new Nested
                {
                    Inner = new Nested {Inner = new Nested {Property1 = "value1", Property2 = "value2"}}
                });
            sourceObject.EnsureCannotBeChangeAndEnableSnapshotting();
            var snapsnot = (RavenJObject) sourceObject.CreateSnapshot();

            var innerInner = (RavenJObject)snapsnot.SelectTokenWithRavenSyntaxReturningFlatStructure("Inner.Inner", true).First().Item1;

            innerInner["Property1"] = "value3";

            Assert.Equal("value1", sourceObject.Value<RavenJObject>("Inner").Value<RavenJObject>("Inner")["Property1"].Value<string>());
        }

        [Fact]
        public void ShouldPatchMultipleNestedProperties()
        {
            using (var store = NewDocumentStore(requestedStorage: "esent"))
            {
                var test1 = new Nested { Inner = new Nested { Inner = new Nested { Property1 = "value1", Property2 = "value2" } } };

                using (var session = store.OpenSession())
                {
                    session.Store(test1);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.UseOptimisticConcurrency = true;
                    session.Advanced.Defer(
                    new PatchCommandData
                    {
                        Key = test1.Id,
                        Patches = new[] {
                            new PatchRequest 
                            { 
                                Type = PatchCommandType.Modify, 
                                Name = "Inner.Inner", 
                                Nested = new[] {
                                    new PatchRequest {
                                        Type = PatchCommandType.Set,
                                        Name = "Property1",
                                        Value = "value3" 
                                    },
                                    new PatchRequest {
                                        Type = PatchCommandType.Set,
                                        Name = "Property2",
                                        Value = "value4" 
                                    }
                                }
                            }

                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var test2 = session.Load<Nested>(test1.Id);

                    Assert.Equal("value3", test2.Inner.Inner.Property1);
                    Assert.Equal("value4", test2.Inner.Inner.Property2);
                }
            }
        }
    }
}