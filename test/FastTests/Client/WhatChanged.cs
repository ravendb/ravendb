using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class WhatChanged : RavenTestBase
    {
        public WhatChanged(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void What_Changed_New_Field()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new BasicName()
                    {
                        Name = "Toli"
                    } 
                    , "users/1");
                  
                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }
                using (var newSession = store.OpenSession())
                {
                    var user = newSession.Load<NameAndAge>("users/1");
                    user.Age = 5;
                    var changes = newSession.Advanced.WhatChanged();
                    Assert.Equal(changes["users/1"].Length, 1);
                    Assert.Equal(changes["users/1"][0].Change, DocumentsChanges.ChangeType.NewField);
                    newSession.SaveChanges();
                }
            }
        }
        
        [Fact]
        public void What_Changed_Removed_Field()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Conventions.PreserveDocumentPropertiesNotFoundOnModel = false;
                } 
            }))
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new NameAndAge()
                    {
                        Name = "Toli",
                        Age = 5
                    }
                    , "users/1");

                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    newSession.Load<BasicAge>("users/1");
                    var changes = newSession.Advanced.WhatChanged();
                    Assert.Equal(changes["users/1"].Length, 1);
                    Assert.Equal(changes["users/1"][0].Change, DocumentsChanges.ChangeType.RemovedField);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void What_Changed_Change_Field()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Conventions.PreserveDocumentPropertiesNotFoundOnModel = false;
                } 
            }))
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new BasicAge()
                    {
                        Age = 5
                    }
                    , "users/1");

                    Assert.Equal(newSession.Advanced.WhatChanged().Count, 1);
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    newSession.Load<Int>("users/1");
                    var changes = newSession.Advanced.WhatChanged();
                    Assert.Equal(changes["users/1"].Length, 2);
                    Assert.Equal(changes["users/1"][0].Change, DocumentsChanges.ChangeType.RemovedField);
                    Assert.Equal(changes["users/1"][1].Change, DocumentsChanges.ChangeType.NewField);
                    newSession.SaveChanges();
                }
            }
        }

        [Fact]
        public void What_Changed_Array_Value_Changed()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new Arr()
                        {
                            Array = new [] { (dynamic)"a", 1, "b"}
                        }
                        , "users/1");
                    var changes = newSession.Advanced.WhatChanged();

                    Assert.Equal(1, changes.Count);
                    Assert.Equal(1, changes["users/1"].Length);
                    Assert.Equal(DocumentsChanges.ChangeType.DocumentAdded, changes["users/1"][0].Change);
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var arr = newSession.Load<Arr>("users/1");
                    arr.Array = new[] {(dynamic)"a", 2, "c"};

                    var changes = newSession.Advanced.WhatChanged();
                    Assert.Equal(1, changes.Count);
                    Assert.Equal(2, changes["users/1"].Length);

                    Assert.Equal(DocumentsChanges.ChangeType.ArrayValueChanged, changes["users/1"][0].Change);
                    Assert.Equal(1L, changes["users/1"][0].FieldOldValue);
                    Assert.Equal(2L, changes["users/1"][0].FieldNewValue);

                    Assert.Equal(DocumentsChanges.ChangeType.ArrayValueChanged, changes["users/1"][1].Change);
                    Assert.Equal("b", changes["users/1"][1].FieldOldValue.ToString());
                    Assert.Equal("c", changes["users/1"][1].FieldNewValue.ToString());
                }
            }
        }

        [Fact]
        public void What_Changed_Array_Value_Added()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new Arr
                    {
                        Array = new[] {(dynamic)"a", 1, "b"}
                    }, "arr/1");
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var arr = newSession.Load<Arr>("arr/1");
                    arr.Array = new[] {(dynamic)"a", 1, "b", "c", 2};

                    var changes = newSession.Advanced.WhatChanged();
                    Assert.Equal(1, changes.Count);
                    Assert.Equal(2, changes["arr/1"].Length);

                    Assert.Equal(DocumentsChanges.ChangeType.ArrayValueAdded, changes["arr/1"][0].Change);
                    Assert.Null(changes["arr/1"][0].FieldOldValue);
                    Assert.Equal("c", changes["arr/1"][0].FieldNewValue.ToString());

                    Assert.Equal(DocumentsChanges.ChangeType.ArrayValueAdded, changes["arr/1"][1].Change);
                    Assert.Null(changes["arr/1"][1].FieldOldValue);
                    Assert.Equal(2L, changes["arr/1"][1].FieldNewValue);
                }
            }
        }

        [Fact]
        public void What_Changed_Array_Value_Removed()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new Arr
                    {
                        Array = new[] { (dynamic)"a", 1, "b" }
                    }, "arr/1");
                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    var arr = newSession.Load<Arr>("arr/1");
                    arr.Array = new[] { (dynamic)"a"};

                    var changes = newSession.Advanced.WhatChanged();
                    Assert.Equal(1, changes.Count);
                    Assert.Equal(2, changes["arr/1"].Length);

                    Assert.Equal(DocumentsChanges.ChangeType.ArrayValueRemoved, changes["arr/1"][0].Change);
                    Assert.Equal(1L, changes["arr/1"][0].FieldOldValue);
                    Assert.Null(changes["arr/1"][0].FieldNewValue);

                    Assert.Equal(DocumentsChanges.ChangeType.ArrayValueRemoved, changes["arr/1"][1].Change);
                    Assert.Equal("b", changes["arr/1"][1].FieldOldValue.ToString());
                    Assert.Null(changes["arr/1"][0].FieldNewValue);
                }
            }
        }

        [Fact]
        public void RavenDB_8169()
        {
            //Test that when old and new values are of different type
            //but have the same value, we consider them unchanged

            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new Int
                    {
                        Number = 1
                    }, "num/1");

                    newSession.Store(new Double
                    {
                        Number = 2.0
                    }, "num/2");

                    newSession.SaveChanges();
                }

                using (var newSession = store.OpenSession())
                {
                    newSession.Load<Double>("num/1");                    
                    var changes = newSession.Advanced.WhatChanged();

                    Assert.Equal(0 , changes.Count);
                }

                using (var newSession = store.OpenSession())
                {
                    newSession.Load<Int>("num/2");
                    var changes = newSession.Advanced.WhatChanged();

                    Assert.Equal(0, changes.Count);
                }
            }
        }

        [Fact]
        public void WhatChanged_should_be_idempotent_operation()
        {
            //RavenDB-9150

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "user1" }, "users/1");
                    session.Store(new User { Name = "user2", Age = 1 }, "users/2");
                    session.Store(new User { Name = "user3", Age = 1}, "users/3");

                    Assert.Equal(3, session.Advanced.WhatChanged().Count);
                    session.SaveChanges();

                    var user1 = session.Load<User>("users/1");
                    var user2 = session.Load<User>("users/2");

                    user1.Age = 10;
                    session.Delete(user2);

                    Assert.Equal(2, session.Advanced.WhatChanged().Count);
                    Assert.Equal(2, session.Advanced.WhatChanged().Count);

                }
            }
        }

        [Fact]
        public void WhatChanged_Delete_After_Change_Value()
        {
            //RavenDB-13501
            using (var store = GetDocumentStore())
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

        [Fact]
        public void RemovingAndAddingSameAmountOfFieldsToObjectShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                const string docID = "d/1";
                using (var session = store.OpenSession())
                {
                    var d = new Doc();
                    session.Store(d, docID);
                    var meta = session.Advanced.GetMetadataFor(d);

                    meta["Test-A"] = new[] { "a", "a", "a" };
                    meta["Test-C"] = new[] { "c", "c", "c" };
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var d = session.Load<Doc>(docID);
                    var meta = session.Advanced.GetMetadataFor(d);

                    meta["Test-A"] = new[] { "b", "a", "c" };

                    var changes = session.Advanced.WhatChanged();
                    Assert.True(changes.Count == 1);
                    Assert.True(changes[docID].Length == 5);
                    Assert.True(changes[docID][3].Change == DocumentsChanges.ChangeType.ArrayValueChanged);
                    Assert.True(changes[docID][3].FieldName == "Test-A");
                    Assert.True(changes[docID][4].Change == DocumentsChanges.ChangeType.ArrayValueChanged);
                    Assert.True(changes[docID][4].FieldName == "Test-A");
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
                    meta["Test-B"] = new[] { "b", "b", "b" };
                    meta["Test-D"] = new[] { "d", "d", "d" };

                    var changes = session.Advanced.WhatChanged();
                    Assert.True(changes.Count == 1);
                    Assert.True(changes[docID].Length == 7);
                    Assert.True(changes[docID].Single(x=>x.FieldName == "Test-A").Change == DocumentsChanges.ChangeType.RemovedField);
                    Assert.True(changes[docID].Single(x=>x.FieldName == "Test-C").Change == DocumentsChanges.ChangeType.RemovedField);
                    Assert.True(changes[docID].Single(x=>x.FieldName == "Test-B").Change == DocumentsChanges.ChangeType.NewField);
                    Assert.True(changes[docID].Single(x=>x.FieldName == "Test-D").Change == DocumentsChanges.ChangeType.NewField);
                }

                using (var session = store.OpenSession())
                {
                    var d = session.Load<Doc>(docID);
                    var meta = session.Advanced.GetMetadataFor(d);


                    meta.Remove("Test-A");
                    meta["Test-B"] = new[] { "b", "b", "b" };

                    var changes = session.Advanced.WhatChanged();
                    Array.Sort(changes[docID], (x, y) => x.FieldName.CompareTo(y.FieldName));
                    Assert.True(changes.Count == 1);
                    Assert.True(changes[docID].Single(x=>x.FieldName == "Test-A").Change == DocumentsChanges.ChangeType.RemovedField);
                    Assert.True(changes[docID].Single(x=>x.FieldName == "Test-B").Change == DocumentsChanges.ChangeType.NewField);
                }
            }
        }

        [Fact]
        public void CanSeeChangesWhenAddingGuidsToArray()
        {
            using (var store = GetDocumentStore())
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
    }

    public class Doc
    {
        public Doc()
        {
            SomeGuids = new List<Guid>();
        }

        public List<Guid> SomeGuids { get; set; }
    }

    public class BasicName
    {
        public string Name { set; get; }
    }

    public class NameAndAge
    {
        public string Name { set; get; }
        public int Age { set; get; }
    }

    public class BasicAge
    {
        public int Age { set; get; }
    }

    class TestObject
    {
        public string Id { get; set; }
        public string A { get; set; }
        public string B { get; set; }
    }

    public class Int
    {
        public int Number { set; get; }
    }

    public class Double
    {
        public double Number { set; get; }
    }

    public class Arr
    {
        public dynamic[] Array { set; get; }
    }
}
