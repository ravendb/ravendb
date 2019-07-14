using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class WhatChangedTests : RavenTest
    {
        [Fact]
        public void WhatChangeSupposeToWorkWithRavenJObject()
        {
            var obj = new { Id = (string)null, PropertyToRemove = true };
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(obj);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ravenObj = session.Load<RavenJObject>(obj.Id);

                    Assert.NotNull(ravenObj);

                    ravenObj.Remove("PropertyToRemove");
                    ravenObj.Add("PropertyToAdd", true);

                    Assert.False(ravenObj.ContainsKey("PropertyToRemove"));
                    Assert.True(ravenObj.ContainsKey("PropertyToAdd"));

                    //Not suppose to throw an exception
                    session.Advanced.WhatChanged();

                }
            }

        }

        [Fact]
        public void WhatChanged_Delete_After_Change_Value()
        {
            //RavenDB-13501
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    const string id = "ABC";
                    var o = new TestObject();
                    o.Id = id;
                    o.A = "A";
                    o.B = "A";
                    session.Store(o);
                    session.SaveChanges();
                    Assert.True(!session.Advanced.HasChanges);

                    o = session.Load<TestObject>(id);
                    o.A = "B";
                    o.B = "C";
                    session.Delete(o);

                    var whatChanged = session.Advanced.WhatChanged();

                    Assert.True(whatChanged.Count == 1
                                && whatChanged.Values.First()[0].Change == DocumentsChanges.ChangeType.DocumentDeleted);

                    session.SaveChanges();

                    o = session.Load<TestObject>(id);
                    Assert.True(o == null);
                }
            }
        }

        class TestObject
        {
            public string Id { get; set; }
            public string A { get; set; }
            public string B { get; set; }
        }

        [Fact]
        public void RemovingAndAddingSameAmountOfFieldsToObjectShouldWork()
        {
            using (var store = NewDocumentStore())
            {
                const string docID = "d/1";
                using (var session = store.OpenSession())
                {
                    var d = new Doc();
                    session.Store(d, docID);
                    var meta = session.Advanced.GetMetadataFor(d);

                    meta["Test-A"] = RavenJToken.FromObject(new[] { "a", "a", "a" });
                    meta["Test-C"] = RavenJToken.FromObject(new[] { "c", "c", "c" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var d = session.Load<Doc>(docID);
                    var meta = session.Advanced.GetMetadataFor(d);

                    meta["Test-A"] = RavenJToken.FromObject(new[] { "b", "a", "c" });

                    var changes = session.Advanced.WhatChanged();
                    Assert.True(changes.Count == 1 && changes.Values.First()[0].Change == DocumentsChanges.ChangeType.FieldChanged);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var d = session.Load<Doc>(docID);
                    var meta = session.Advanced.GetMetadataFor(d);

                    meta.Remove("Test-A");

                    var changes = session.Advanced.WhatChanged();
                    Assert.True(changes.Count == 1 && changes.Values.First()[0].Change == DocumentsChanges.ChangeType.RemovedField);
                }

                using (var session = store.OpenSession())
                {
                    var d = session.Load<Doc>(docID);
                    var meta = session.Advanced.GetMetadataFor(d);

                    meta.Remove("Test-A");
                    meta.Remove("Test-C");
                    meta["Test-B"] = RavenJToken.FromObject(new[] { "b", "b", "b" });
                    meta["Test-D"] = RavenJToken.FromObject(new[] { "d", "d", "d" });

                    var changes = session.Advanced.WhatChanged();
                    Assert.True(changes.Count == 1);
                    Assert.True(changes[docID].Length == 4);
                    Assert.True(changes[docID][0].Change == DocumentsChanges.ChangeType.NewField);
                    Assert.True(changes[docID][1].Change == DocumentsChanges.ChangeType.RemovedField);
                    Assert.True(changes[docID][2].Change == DocumentsChanges.ChangeType.NewField);
                    Assert.True(changes[docID][3].Change == DocumentsChanges.ChangeType.RemovedField);
                }

                using (var session = store.OpenSession())
                {
                    var d = session.Load<Doc>(docID);
                    var meta = session.Advanced.GetMetadataFor(d);


                    meta.Remove("Test-A");
                    meta["Test-B"] = RavenJToken.FromObject(new[] { "b", "b", "b" });

                    var changes = session.Advanced.WhatChanged();
                    Assert.True(changes.Count == 1);
                    Assert.True(changes[docID][0].Change == DocumentsChanges.ChangeType.NewField);
                    Assert.True(changes[docID][1].Change == DocumentsChanges.ChangeType.RemovedField);
                }
            }
        }

        [Fact]
        public void CanSeeChangesWhenAddingGuidsToArray()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var d = new Doc();
                    d.SomeGuids.Add(Guid.NewGuid());
                    session.Store(d, "d/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var d = session.Load<Doc>("d/1");
                    d.SomeGuids.Add(Guid.NewGuid());

                    var changes = session.Advanced.WhatChanged();
                    Assert.True(changes.Count == 1 && changes.Values.First()[0].Change == DocumentsChanges.ChangeType.ArrayValueAdded);
                }
            }
        }

        public class Doc
        {
            public Doc()
            {
                SomeGuids = new List<Guid>();
            }

            public List<Guid> SomeGuids { get; set; }
        }
    }
}
