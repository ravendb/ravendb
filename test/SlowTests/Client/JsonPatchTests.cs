using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using FastTests;
using Microsoft.AspNetCore.JsonPatch;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Orders;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class JsonPatchTests : RavenTestBase
    {
        public JsonPatchTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void PatchingWithEscaping()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                IDictionary<string, object> originalCompany = new ExpandoObject();

                originalCompany["~"] = "~";
                originalCompany["/"] = "/";
                originalCompany["foo/bar~"] = "foo/bar~";

                var list = new List<ExpandoObject>();
                IDictionary<string, object> prop1 = new ExpandoObject();
                prop1["~"] = "Nested~";
                list.Add((ExpandoObject)prop1);
                IDictionary<string, object> prop2 = new ExpandoObject();
                prop2["/"] = "Nested/";
                list.Add((ExpandoObject)prop2);
                IDictionary<string, object> prop3 = new ExpandoObject();
                prop3["foo/bar~"] = "NestedFoo/Bar~";
                list.Add((ExpandoObject)prop3);

                originalCompany["biscuits"] = list;

                dynamic originalCompany2 = originalCompany;
                using (var session = store.OpenSession())
                {
                    session.Store(originalCompany2);
                    documentId = originalCompany2.Id;
                    session.SaveChanges();
                }
               
                var jpd = new JsonPatchDocument();
                jpd.Add("/~0", "Hibernating Rhinos1");
                jpd.Replace("/~1", "Hibernating Rhinos2");
                jpd.Add("/foo~1bar~0", "Hibernating Rhinos3");
                jpd.Add("/biscuits/0/~0", "Hibernating Rhinos1");
                jpd.Add("/biscuits/1/~1", "Hibernating Rhinos2");
                jpd.Replace("/biscuits/1/~1", "Hibernating Rhinos replaced");
                jpd.Add("/biscuits/2/foo~1bar~0", "Hibernating Rhinos3");
                JsonPatchResult op = store.Operations.Send(new JsonPatchOperation(documentId, jpd));
                Assert.Equal(PatchStatus.Patched, op.Status);

                using (var session = store.OpenSession())
                {
                    IDictionary<string, object> dbCompany = session.Load<ExpandoObject>(documentId);
                    jpd.ApplyTo(originalCompany2);
                    AssertExpandosEqual(originalCompany2, (dynamic)dbCompany);
                }
            }
        }

        [Fact]
        public void PatchingWithAdd()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                dynamic originalCompany = new ExpandoObject();
                originalCompany.Name = "The Wall";

                using (var session = store.OpenSession())
                {
                    session.Store(originalCompany);
                    documentId = originalCompany.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Add("/Name", "Hibernating Rhinos");

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    var dbCompany = session.Load<ExpandoObject>(documentId);
                    jpd.ApplyTo(originalCompany);

                    AssertExpandosEqual(originalCompany, dbCompany);
                }
            }
        }

        [Fact]
        public void PatchingWithAddNonExistentDocId()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                dynamic originalCompany = new ExpandoObject();
                originalCompany.Name = "The Wall";

                using (var session = store.OpenSession())
                {
                    session.Store(originalCompany);
                    documentId = originalCompany.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Add("/Name", "Hibernating Rhinos");

                var error = Assert.ThrowsAny<RavenException>(() => store.Operations.Send(new JsonPatchOperation(documentId + "1", jpd)));
                Assert.Contains("Cannot apply json patch because the document ExpandoObjects/1-A1 does not exist", error.Message);
            }
        }

        [Fact]
        public void PatchingWithReplaceNestedPathEscaping()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                IDictionary<string, object> originalCompany = new ExpandoObject();
                originalCompany["/"] = new ExpandoObject();
                ((IDictionary<string,object>)originalCompany["/"])["Name"] = "Hibernating";

                using (var session = store.OpenSession())
                {
                    session.Store((dynamic)originalCompany);
                    documentId = ((dynamic)originalCompany).Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Replace("/~1/Name", "Developer");

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbCompany = session.Load<ExpandoObject>(documentId);
                    jpd.ApplyTo(originalCompany);

                    AssertExpandosEqual((dynamic)originalCompany, dbCompany);
                }
            }
        }

        [Fact]
        public void PatchingWithAddNestedPath()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                dynamic originalCompany = new ExpandoObject();
                originalCompany.Contact = new ExpandoObject();
                originalCompany.Contact.Name = "Hibernating";

                using (var session = store.OpenSession())
                {

                    session.Store(originalCompany);
                    documentId = originalCompany.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Add("/Contact/Title", "Developer");

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbCompany = session.Load<ExpandoObject>(documentId);
                    jpd.ApplyTo(originalCompany);

                    AssertExpandosEqual(originalCompany, dbCompany);
                }
            }
        }

        private void AssertExpandosEqual(ExpandoObject expected, ExpandoObject dbObj)
        {
            var dbDictionary = (IDictionary<string, object>)dbObj;
            dbDictionary.Remove("@metadata");
            dbDictionary.Remove("$type");
            var expectedDictionary = (IDictionary<string, object>)expected;

            Assert.Equal(expectedDictionary.Count, dbDictionary.Count);
            foreach (KeyValuePair<string, object> expectedEntry in expectedDictionary)
            {
                var expectedEl = expectedEntry.Value;
                var actualEl = dbDictionary[expectedEntry.Key];
                if (actualEl is JArray arr)
                {
                    actualEl = arr.ToObject<List<object>>();
                }

                if (expectedEl is ExpandoObject ex && actualEl is ExpandoObject act)
                {
                    AssertExpandosEqual(ex, act);
                }
                else if (expectedEl is List<object> exList && actualEl is List<object> actList)
                {
                    AssertListsEqual(exList, actList);
                }
                else if (decimal.TryParse(expectedEl?.ToString(), out decimal num1) && decimal.TryParse(actualEl?.ToString(), out decimal num2))
                {
                    Assert.Equal(num1, num2);
                }
                else
                {
                    Assert.Equal(expectedEl, actualEl);
                }
            }
        }

        private void AssertListsEqual(IList expectedList, IList actualList)
        {
            for (int j = 0; j < expectedList.Count; j++)
            {
                var actItem = actualList[j];
                if (actItem is JObject obj)
                {
                    actItem = obj.ToObject<ExpandoObject>();
                }
                if (expectedList[j] is ExpandoObject obj1 && actItem is ExpandoObject obj2)
                {
                    AssertExpandosEqual(obj1, obj2);
                }
                else if (expectedList[j] is IList exList && actItem is IList actList)
                {
                    AssertListsEqual(exList, actList);
                }
                else if (decimal.TryParse(actItem?.ToString(), out decimal num1) && decimal.TryParse(actItem?.ToString(), out decimal num2))
                {
                    Assert.Equal(num1, num2);
                }
                else
                {
                    Assert.Equal(expectedList[j], actItem);
                }
            }
        }

        [Fact]
        public void PatchingWithAddNestedPathDoesntExist()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                dynamic originalCompany = new ExpandoObject();
                originalCompany.Contact = new ExpandoObject();
                originalCompany.Contact.City = "Hadera";
                originalCompany.Contact.LastName = "Rhinos";

                using (var session = store.OpenSession())
                {
                    session.Store(originalCompany);
                    documentId = originalCompany.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Add("/Contact/FirstName", "Hibernating");

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbCompany = session.Load<ExpandoObject>(documentId);

                    jpd.ApplyTo(originalCompany);

                    AssertExpandosEqual(originalCompany, dbCompany);
                }
            }
        }

        [Fact]
        public void PatchingWithAddMoreThanOneNonexistentProperty()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                var originalCompany = new Company
                {
                    Contact = new Contact
                    {
                        Name = "Rhinos"
                    }
                };
                using (var session = store.OpenSession())
                {
                    session.Store(originalCompany);
                    documentId = originalCompany.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Add("/Contact/FirstName/ExtraProp", "Hiber");

                var error = Assert.ThrowsAny<RavenException>(() => store.Operations.Send(new JsonPatchOperation(documentId, jpd)));
                Assert.Contains("System.ArgumentException: Cannot reach target location. Failed to fetch 'FirstName' in path '/Contact/FirstName/ExtraProp' for operation 'Add'", error.Message);
            }
        }

        [Fact]
        public void PatchingWithAddToArrayAtTheEnd()
        {
            dynamic testObject = new ExpandoObject();
            List<object> list = new();
            list.Add(1);
            list.Add(2);
            list.Add(4);
            testObject.MyArray = list;

            using (var store = GetDocumentStore())
            {
                string documentId = null;


                using (var session = store.OpenSession())
                {
                    session.Store(testObject);
                    documentId = testObject.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Add("/MyArray/-", 5);

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbObject = session.Load<ExpandoObject>(documentId);
                    jpd.ApplyTo(testObject);

                    AssertExpandosEqual(testObject, dbObject);
                }
            }
        }

        [Fact]
        public void PatchingWithAddToArrayAtIndex()
        {
            dynamic testObject = new ExpandoObject();
            List<object> list = new();
            list.Add(1);
            list.Add(2);
            list.Add(4);
            testObject.MyArray = list;

            using (var store = GetDocumentStore())
            {
                string documentId = null;

                using (var session = store.OpenSession())
                {
                    session.Store(testObject);
                    documentId = testObject.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Add("/MyArray/2", 3);

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbObject = session.Load<ExpandoObject>(documentId);
                    jpd.ApplyTo(testObject);

                    AssertExpandosEqual(testObject, dbObject);
                }
            }
        }

        public class MyClass
        {
            public List<object> MyArray { get; set; }
            public string Id { get; set; }

            public string Name;
        }

        [Fact]
        public void PatchingWithAddToNestedArraysAtIndex()
        {
            using (var store = GetDocumentStore())
            {
                dynamic myClass = new ExpandoObject();
                myClass.MyArray = new List<object> { 1, 2, new List<int> { 22, 23 } };

                string documentId = null;

                using (var session = store.OpenSession())
                {
                    session.Store(myClass);
                    documentId = myClass.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Add("/MyArray/2/1", 100);
                jpd.ApplyTo(myClass);

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic changedClass = session.Load<ExpandoObject>(documentId);
                    AssertExpandosEqual(myClass, changedClass);
                }
            }
        }

        [Fact]
        public void PatchingWithRemove()
        {
            dynamic testObject = new ExpandoObject();
            testObject.Name = "The Wall";
            testObject.City = "Hadera";

            using (var store = GetDocumentStore())
            {
                string documentId = null;

                using (var session = store.OpenSession())
                {
                    session.Store(testObject);
                    documentId = testObject.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Remove("/Name");

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbObject = session.Load<ExpandoObject>(documentId);
                    jpd.ApplyTo(testObject);

                    AssertExpandosEqual(testObject, dbObject);
                }
            }
        }

        [Fact]
        public void PatchingWithRemoveArrayElement()
        {
            dynamic testObject = new ExpandoObject();
            List<object> list = new();
            list.Add(1);
            list.Add(2);
            list.Add(3);
            testObject.MyArray = list;

            using (var store = GetDocumentStore())
            {
                string documentId = null;

                using (var session = store.OpenSession())
                {
                    session.Store(testObject);
                    documentId = testObject.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Remove("/MyArray/1");

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbObject = session.Load<ExpandoObject>(documentId);

                    jpd.ApplyTo(testObject);

                    AssertExpandosEqual(testObject, dbObject);
                }
            }
        }

        [Fact]
        public void PatchingWithReplace()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                dynamic originalCompany = new ExpandoObject();
                originalCompany.Name = "The Wall";

                using (var session = store.OpenSession())
                {
                    session.Store(originalCompany);
                    documentId = originalCompany.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Replace("/Name", "Hibernating Rhinos");

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    var company = session.Load<ExpandoObject>(documentId);
                    jpd.ApplyTo(originalCompany);

                    AssertExpandosEqual(originalCompany, company);
                }
            }
        }

        [Fact]
        public void PatchingWithReplaceAtNonExistent()
        {
            dynamic originalCompany = new ExpandoObject();
            originalCompany.Name = "The Wall";
            originalCompany.Address = new ExpandoObject();
            originalCompany.Address.City = "Netanya";

            using (var store = GetDocumentStore())
            {
                string documentId = null;

                using (var session = store.OpenSession())
                {
                    session.Store(originalCompany);
                    documentId = originalCompany.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Replace("/Address/NonexistentProperty", "Hibernating Rhinos");

                var error = Assert.ThrowsAny<RavenException>(() => store.Operations.Send(new JsonPatchOperation(documentId, jpd)));
                Assert.Contains("System.ArgumentException: Cannot reach target location. Failed to fetch 'NonexistentProperty' in path '/Address/NonexistentProperty' for operation 'Replace'", error.Message);
            }
        }

        [Fact]
        public void PatchingWithReplaceArrayElement()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                var originalObject = new MyTestClass { MyArray = new List<int>() };
                originalObject.MyArray.Add(1);
                originalObject.MyArray.Add(2);
                originalObject.MyArray.Add(4);

                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Replace("/MyArray/1", 100);

                var converter = new ExpandoObjectConverter();
                var json = JsonConvert.SerializeObject(originalObject);
                dynamic obj = JsonConvert.DeserializeObject<ExpandoObject>(json, converter);

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbObject = session.Load<ExpandoObject>(documentId);

                    jpd.ApplyTo(obj);

                    AssertExpandosEqual(obj, dbObject);
                }
            }
        }

        [Fact]
        public void PatchingWithMove()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                var originalObject = new MyTestClass { Name = "Hibernating" };
                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Move("/Name", "/NewPropName");

                var converter = new ExpandoObjectConverter();
                var json = JsonConvert.SerializeObject(originalObject);
                dynamic obj = JsonConvert.DeserializeObject<ExpandoObject>(json, converter);

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbObject = session.Load<ExpandoObject>(documentId);
                    var dictionary = (IDictionary<string, object>)dbObject;
                    jpd.ApplyTo(obj);

                    AssertExpandosEqual(obj, dbObject);
                    Assert.Equal(obj.NewPropName, dictionary["NewPropName"]);
                    Assert.True(dictionary.ContainsKey("Name") == false);
                }
            }
        }

        [Fact]
        public void PatchingWithMoveToNestedInSameObject()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                var originalObject = new MyTestClass { Name = "Hibernating" };
                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Move("/Name", "/Name/CompanyName");

                var converter = new ExpandoObjectConverter();
                var json = JsonConvert.SerializeObject(originalObject);
                dynamic obj = JsonConvert.DeserializeObject<ExpandoObject>(json, converter);

                Assert.ThrowsAny<RavenException>(() => store.Operations.Send(new JsonPatchOperation(documentId, jpd)));

                Assert.ThrowsAny<Exception>(() => jpd.ApplyTo(obj));

            }
        }

        [Fact]
        public void PatchingWithMoveArrayElementFromIndexToIndex()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                dynamic originalObject = new ExpandoObject();
                originalObject.MyArray = new List<int> { 20, 40, 10, 30 };

                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();

                    var jpd = new JsonPatchDocument();
                    jpd.Move("/MyArray/0", "/MyArray/2");
                    jpd.Move("/MyArray/0", "/MyArray/3");

                    jpd.ApplyTo(originalObject);

                    store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                    var dbObject = session.Load<ExpandoObject>(documentId);

                    AssertExpandosEqual(originalObject, dbObject);
                }
            }
        }

        [Fact]
        public void PatchingWithMultipleAddReplace()
        {
            dynamic originalObject = new ExpandoObject();
            originalObject.Name = "Hibernating";

            using (var store = GetDocumentStore())
            {
                string documentId = null;

                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();
                }

                dynamic address = new ExpandoObject();
                address.City = "Netanya";

                var jpd = new JsonPatchDocument();
                jpd.Add("/Name", address);
                jpd.Replace("/Name/City", "Hadera");

                var converter = new ExpandoObjectConverter();
                var json = JsonConvert.SerializeObject(originalObject);
                dynamic obj = JsonConvert.DeserializeObject<ExpandoObject>(json, converter);

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbObject = session.Load<ExpandoObject>(documentId);

                    jpd.ApplyTo(obj);

                    AssertExpandosEqual(obj, dbObject);
                }
            }
        }

        [Fact]
        public void PatchingWithMultipleAddRemoveObjects()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                var originalObject = new MyTestClass { };
                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Add("/Name", new Address { City = "Netanya" });
                jpd.Remove("/Name");

                var converter = new ExpandoObjectConverter();
                var json = JsonConvert.SerializeObject(originalObject);
                dynamic obj = JsonConvert.DeserializeObject<ExpandoObject>(json, converter);

                var res = store.Operations.Send(new JsonPatchOperation(documentId, jpd));

                using (var session = store.OpenSession())
                {
                    dynamic dbObject = session.Load<ExpandoObject>(documentId);
                    var dictionary = (IDictionary<string, object>)dbObject;

                    jpd.ApplyTo(obj);

                    Assert.True(dictionary.ContainsKey("Name") == false);
                }
            }
        }

        [Fact]
        public void PatchingWithTestAsSingleOperation()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                var originalCompany = new Company {Name = "The Wall"};
                using (var session = store.OpenSession())
                {
                    session.Store(originalCompany);
                    documentId = originalCompany.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Test("/Name", "The Wal");

                var error = Assert.ThrowsAny<RavenException>(() => store.Operations.Send(new JsonPatchOperation(documentId, jpd)));
                Assert.Contains("System.InvalidOperationException: The current value 'The Wall' is not equal to the test value 'The Wal'", error.Message);
            }
        }

        [Fact]
        public void PatchingWithTestMultipleTests()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer => serializer.TypeNameHandling = TypeNameHandling.None
                    };
                }
            }))
            {
                var list = new Dictionary<string, object>
                {
                    {"Id", null },
                    {"Float", (float)13},
                    {"Double", (double)13},
                    {"Decimal", 13M},
                    {"Long", (long)13},
                    {"String", "The Wall"}, 
                    {"Boolean1", false}, 
                    {"Boolean2", true},
                    {"Contact", new { Name = "Stav" }},
                };

                string documentId = null;
                var originalObject = new ExpandoObject() as IDictionary<string, object>;

                foreach (var obj in list)
                {
                    originalObject.Add(obj.Key, obj.Value);
                }
                originalObject.Add("List", list);

                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = (string)originalObject["Id"];
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Test("/List", list);

                foreach (var obj in list)
                {
                    if (obj.Key != "Id")
                    {
                        jpd.Test("/" + obj.Key, obj.Value);
                    }
                }

                store.Operations.Send(new JsonPatchOperation(documentId, jpd));
            }
        }

        [Fact]
        public void PatchingWithTestAsSingleOperationNumericVsString()
        {
            using (var store = GetDocumentStore())
            {
                string documentId = null;
                var originalCompany = new Company { Name = "1" };
                using (var session = store.OpenSession())
                {
                    session.Store(originalCompany);
                    documentId = originalCompany.Id;
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();

                jpd.Test("/Name", 1);

                var error = Assert.ThrowsAny<RavenException>(() => store.Operations.Send(new JsonPatchOperation(documentId, jpd)));
                Assert.Contains("System.InvalidOperationException: The current value '1' is not equal to the test value '1'", error.Message);
            }
        }

        [Fact]
        public void PatchingWithTestMultipleOperations()
        {
            var originalObject = new MyClass
            {
                Name = "Hibernating",
                MyArray = new List<object> { 1, 2, 5 }
            };
            var innerArrObject = new SinglePropClass { City = "Netanya" };

            using (var store = GetDocumentStore())
            {
                string documentId = null;

                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();

                    var jpdLocal = new JsonPatchDocument();
                    var jpd = new JsonPatchDocument();

                    jpdLocal.Replace("/MyArray/1", innerArrObject);
                    jpdLocal.Replace("/MyArray/1/City", "Hadera");
                    jpdLocal.ApplyTo(originalObject);

                    jpd.Replace("/MyArray/1", innerArrObject);
                    jpd.Replace("/MyArray/1/City", "Hadera");

                    jpd.Test("/MyArray", originalObject.MyArray);

                    store.Operations.Send(new JsonPatchOperation(documentId, jpd));
                }
            }
        }

        [Fact]
        public void PatchingWithTestMultipleOperationsFailure()
        {
            dynamic originalObject = new ExpandoObject();
            originalObject.Name = "Hibernating";
            originalObject.MyArray = new List<object> {1, 2, 5};

            var innerArrObject = new SinglePropClass { City = "Netanya" };

            using (var store = GetDocumentStore())
            {
                string documentId = null;

                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();

                    var jpdLocal = new JsonPatchDocument();
                    var jpd = new JsonPatchDocument();

                    jpdLocal.Replace("/MyArray/1", innerArrObject);
                    jpdLocal.ApplyTo(originalObject);

                    jpd.Replace("/MyArray/1", innerArrObject);
                    jpd.Replace("/MyArray/1/City", "Hadera");

                    jpd.Test("/MyArray", originalObject.MyArray);

                    var error = Assert.ThrowsAny<RavenException>(() => store.Operations.Send(new JsonPatchOperation(documentId, jpd)));
                    Assert.Contains("The current value '[1,{\"City\":\"Hadera\"},5]' is not equal to the test value '[1,{\"City\":\"Netanya\"},5]", error.Message);

                    //operations should not be applied in case of failure
                    var dbObject = session.Load<dynamic>(originalObject.Id);
                    AssertExpandosEqual(originalObject, dbObject);
                }
            }
        }

        public class SinglePropClass
        {
            public string City;
        }

        [Fact]
        public void PatchingWithDeferSimpleAdd()
        {
            dynamic originalObject = new ExpandoObject();
            originalObject.Name = "Hibernating";

            using (var store = GetDocumentStore())
            {
                string documentId;

                dynamic address = new ExpandoObject();
                address.City = "Netanya";

                var jpd = new JsonPatchDocument();
                jpd.Add("/Name", address);

                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();

                    session.Advanced.Defer(new JsonPatchCommandData(documentId, jpd));
                    session.SaveChanges();

                    dynamic dbObject = session.Load<ExpandoObject>(documentId);

                    jpd.ApplyTo(originalObject);

                    AssertExpandosEqual(originalObject, dbObject);
                }
            }
        }

        [Fact]
        public void PatchingWithDeferInsertToArray()
        {
            dynamic originalObject = new ExpandoObject();
            originalObject.Name = "Hibernating";
            originalObject.MyArray = new List<object> { 1, 2, 3 };

            dynamic originalObject2 = new ExpandoObject();
            originalObject2.Name = "Hibernating";
            originalObject2.MyArray = new List<object> { 1, 2, 3 };

            using (var store = GetDocumentStore())
            {
                string documentId;

                dynamic address = new ExpandoObject();
                address.City = "Netanya";

                var jpd = new JsonPatchDocument();
                jpd.Add("/MyArray/2", address);

                using (var session = store.OpenSession())
                {
                    session.Store(originalObject);
                    documentId = originalObject.Id;
                    session.SaveChanges();

                    originalObject2.Id = documentId;

                    session.Advanced.Defer(new JsonPatchCommandData(documentId, jpd));
                    session.SaveChanges();

                    dynamic dbObject = session.Load<ExpandoObject>(documentId);

                    jpd.ApplyTo(originalObject2);

                    AssertExpandosEqual(originalObject2, dbObject);
                }
            }
        }

        private class MyTestClass
        {
#pragma warning disable 649
            public string Id;
#pragma warning restore 649
            public string Name;
            public List<int> MyArray;
        }
    }
}
