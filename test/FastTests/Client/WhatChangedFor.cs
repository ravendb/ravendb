using System;
using System.Linq;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client;

public class WhatChangedFor : RavenTestBase
{
    public WhatChangedFor(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_New_Field()
    {
        using (var store = GetDocumentStore())
        {
            using (var newSession = store.OpenSession())
            {
                BasicName basicName;
                newSession.Store(basicName = new BasicName()
                {
                    Name = "Toli"
                }
                , "users/1");

                Assert.Equal(newSession.Advanced.WhatChangedFor(basicName).Length, 1);
                newSession.SaveChanges();
            }
            using (var newSession = store.OpenSession())
            {
                var user = newSession.Load<NameAndAge>("users/1");
                user.Age = 5;
                var changes = newSession.Advanced.WhatChangedFor(user);
                Assert.Equal(changes.Length, 1);
                Assert.Equal(changes[0].Change, DocumentsChanges.ChangeType.NewField);
                newSession.SaveChanges();
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_Removed_Field()
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
                NameAndAge nameAndAge;

                newSession.Store(nameAndAge = new NameAndAge()
                {
                    Name = "Toli",
                    Age = 5
                }
                , "users/1");

                Assert.Equal(newSession.Advanced.WhatChangedFor(nameAndAge).Length, 1);
                newSession.SaveChanges();
            }

            using (var newSession = store.OpenSession())
            {
                var user = newSession.Load<BasicAge>("users/1");
                var changes = newSession.Advanced.WhatChangedFor(user);
                Assert.Equal(changes.Length, 1);
                Assert.Equal(changes[0].Change, DocumentsChanges.ChangeType.RemovedField);
                newSession.SaveChanges();
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_Change_Field()
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
                BasicAge basicAge;
                newSession.Store(basicAge = new BasicAge()
                {
                    Age = 5
                }
                , "users/1");

                Assert.Equal(newSession.Advanced.WhatChangedFor(basicAge).Length, 1);
                newSession.SaveChanges();
            }

            using (var newSession = store.OpenSession())
            {
                var user = newSession.Load<Int>("users/1");
                var changes = newSession.Advanced.WhatChangedFor(user);
                Assert.Equal(changes.Length, 2);
                Assert.Equal(changes[0].Change, DocumentsChanges.ChangeType.RemovedField);
                Assert.Equal(changes[1].Change, DocumentsChanges.ChangeType.NewField);
                newSession.SaveChanges();
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_Array_Value_Changed()
    {
        using (var store = GetDocumentStore())
        {
            using (var newSession = store.OpenSession())
            {
                Arr arr;
                newSession.Store(arr = new Arr()
                {
                    Array = new[] { (dynamic)"a", 1, "b" }
                }
                    , "users/1");
                var changes = newSession.Advanced.WhatChangedFor(arr);

                Assert.Equal(1, changes.Length);
                Assert.Equal(DocumentsChanges.ChangeType.DocumentAdded, changes[0].Change);
                newSession.SaveChanges();
            }

            using (var newSession = store.OpenSession())
            {
                var arr = newSession.Load<Arr>("users/1");
                arr.Array = new[] { (dynamic)"a", 2, "c" };

                var changes = newSession.Advanced.WhatChangedFor(arr);
                Assert.Equal(2, changes.Length);

                Assert.Equal(DocumentsChanges.ChangeType.ArrayValueChanged, changes[0].Change);
                Assert.Equal(1L, changes[0].FieldOldValue);
                Assert.Equal(2L, changes[0].FieldNewValue);

                Assert.Equal(DocumentsChanges.ChangeType.ArrayValueChanged, changes[1].Change);
                Assert.Equal("b", changes[1].FieldOldValue.ToString());
                Assert.Equal("c", changes[1].FieldNewValue.ToString());
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_Array_Value_Added()
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
                arr.Array = new[] { (dynamic)"a", 1, "b", "c", 2 };

                var changes = newSession.Advanced.WhatChangedFor(arr);
                Assert.Equal(2, changes.Length);

                Assert.Equal(DocumentsChanges.ChangeType.ArrayValueAdded, changes[0].Change);
                Assert.Null(changes[0].FieldOldValue);
                Assert.Equal("c", changes[0].FieldNewValue.ToString());

                Assert.Equal(DocumentsChanges.ChangeType.ArrayValueAdded, changes[1].Change);
                Assert.Null(changes[1].FieldOldValue);
                Assert.Equal(2L, changes[1].FieldNewValue);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_Array_Value_Removed()
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
                arr.Array = new[] { (dynamic)"a" };

                var changes = newSession.Advanced.WhatChangedFor(arr);
                Assert.Equal(2, changes.Length);

                Assert.Equal(DocumentsChanges.ChangeType.ArrayValueRemoved, changes[0].Change);
                Assert.Equal(1L, changes[0].FieldOldValue);
                Assert.Null(changes[0].FieldNewValue);

                Assert.Equal(DocumentsChanges.ChangeType.ArrayValueRemoved, changes[1].Change);
                Assert.Equal("b", changes[1].FieldOldValue.ToString());
                Assert.Null(changes[0].FieldNewValue);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_RavenDB_8169()
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
                var num = newSession.Load<Double>("num/1");
                var changes = newSession.Advanced.WhatChangedFor(num);

                Assert.Equal(0, changes.Length);
            }

            using (var newSession = store.OpenSession())
            {
                var num = newSession.Load<Int>("num/2");
                var changes = newSession.Advanced.WhatChangedFor(num);

                Assert.Equal(0, changes.Length);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_Delete_After_Change_Value()
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

                var whatChangedFor = session.Advanced.WhatChangedFor(o);

                Assert.True(whatChangedFor.Length == 1 && whatChangedFor[0].Change == DocumentsChanges.ChangeType.DocumentDeleted);

                session.SaveChanges();

                o = session.Load<TestObject>(id);
                Assert.True(o == null);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_RemovingAndAddingSameAmountOfFieldsToObjectShouldWork()
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

                var changes = session.Advanced.WhatChangedFor(d);
                Assert.True(changes.Length == 2);
                Assert.True(changes[0].Change == DocumentsChanges.ChangeType.ArrayValueChanged);
                Assert.True(changes[0].FieldName == "Test-A");
                Assert.True(changes[1].Change == DocumentsChanges.ChangeType.ArrayValueChanged);
                Assert.True(changes[1].FieldName == "Test-A");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var d = session.Load<Doc>(docID);
                var meta = session.Advanced.GetMetadataFor(d);

                meta.Remove("Test-A");

                var changes = session.Advanced.WhatChangedFor(d);
                Assert.True(changes.Length == 1 && changes[0].Change == DocumentsChanges.ChangeType.RemovedField);
            }

            using (var session = store.OpenSession())
            {
                var d = session.Load<Doc>(docID);
                var meta = session.Advanced.GetMetadataFor(d);

                meta.Remove("Test-A");
                meta.Remove("Test-C");
                meta["Test-B"] = new[] { "b", "b", "b" };
                meta["Test-D"] = new[] { "d", "d", "d" };

                var changes = session.Advanced.WhatChangedFor(d);
                Assert.True(changes.Length == 4);
                Assert.True(changes.Single(x => x.FieldName == "Test-A").Change == DocumentsChanges.ChangeType.RemovedField);
                Assert.True(changes.Single(x => x.FieldName == "Test-C").Change == DocumentsChanges.ChangeType.RemovedField);
                Assert.True(changes.Single(x => x.FieldName == "Test-B").Change == DocumentsChanges.ChangeType.NewField);
                Assert.True(changes.Single(x => x.FieldName == "Test-D").Change == DocumentsChanges.ChangeType.NewField);
            }

            using (var session = store.OpenSession())
            {
                var d = session.Load<Doc>(docID);
                var meta = session.Advanced.GetMetadataFor(d);


                meta.Remove("Test-A");
                meta["Test-B"] = new[] { "b", "b", "b" };

                var changes = session.Advanced.WhatChangedFor(d);
                Array.Sort(changes, (x, y) => x.FieldName.CompareTo(y.FieldName));
                Assert.True(changes.Single(x => x.FieldName == "Test-A").Change == DocumentsChanges.ChangeType.RemovedField);
                Assert.True(changes.Single(x => x.FieldName == "Test-B").Change == DocumentsChanges.ChangeType.NewField);
            }
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public void What_Changed_For_CanSeeChangesWhenAddingGuidsToArray()
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

                var changes = session.Advanced.WhatChangedFor(d);
                Assert.True(changes.Length == 1 && changes[0].Change == DocumentsChanges.ChangeType.ArrayValueAdded);
            }
        }
    }
}
