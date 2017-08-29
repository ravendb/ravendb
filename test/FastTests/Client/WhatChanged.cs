using Raven.Client.Documents.Session;
using Xunit;

namespace FastTests.Client
{
    public class WhatChanged : RavenTestBase
    {
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
            using (var store = GetDocumentStore())
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
            using (var store = GetDocumentStore())
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
