using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Threading.Tasks;
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
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client
{
    public class JsonPatchTests : RavenTestBase
    {
        public JsonPatchTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
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

        [RavenFact(RavenTestCategory.Patching)]
        public void PatchingWithTheSameValueLeavesTheDocumentAsItWas()
        {
            using (var store = GetDocumentStore())
            {
                dynamic company = new ExpandoObject();
                company.Name = "The Wall";
                string documentId, changeVector;
                using (var session = store.OpenSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                    documentId = company.Id;
                    changeVector = session.Advanced.GetChangeVectorFor(company);
                }

                var jpd = new JsonPatchDocument();
                jpd.Replace("/Name", "The Wall");
                var result = store.Operations.Send(new JsonPatchOperation(documentId, jpd));
                Assert.Equal(PatchStatus.NotModified, result.Status);

                using (var session = store.OpenSession())
                {
                    var stored = session.Load<ExpandoObject>(documentId);
                    Assert.Equal(changeVector, session.Advanced.GetChangeVectorFor(stored)); // not written again
                }
            }
        }

        [RavenTheory(RavenTestCategory.Patching)]
        [InlineData("@archive-at", "2030-01-01T00:00:00.0000000Z")]
        [InlineData("@nested-object-types", "x")]
        public void PatchingOnlyTheMetadataIsSaved(string key, string value)
        {
            using (var store = GetDocumentStore())
            {
                const string documentId = "people/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Address = new SinglePropClass { City = "Netanya" } }, documentId);
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Add("/@metadata/" + key, value);
                Assert.Equal(PatchStatus.Patched, store.Operations.Send(new JsonPatchOperation(documentId, jpd)).Status);

                using (var session = store.OpenSession())
                {
                    var stored = session.Load<Person>(documentId);
                    Assert.Equal(value, session.Advanced.GetMetadataFor(stored).GetString(key));
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void OperationsSeeTheChangesOfThePreviousOnes()
        {
            dynamic original = new ExpandoObject();
            original.Name = "Hibernating";
            original.Other = "unchanged";
            original.Numbers = new List<int> { 1, 2, 3 };

            using (var store = GetDocumentStore())
            {
                string documentId;
                using (var session = store.OpenSession())
                {
                    session.Store(original);
                    session.SaveChanges();
                    documentId = original.Id;
                }

                dynamic address = new ExpandoObject();
                address.City = "Netanya";

                var jpd = new JsonPatchDocument();
                jpd.Replace("/Name", "Rhinos");
                jpd.Add("/Address", address);
                jpd.Replace("/Address/City", "Hadera");      // inside a value added before
                jpd.Copy("/Name", "/NameCopy");               // from a field replaced before
                jpd.Test("/Other", "unchanged");              // unrelated to the changes
                jpd.Add("/Numbers/0", 0);                     // shifts the indexes of the array
                jpd.Replace("/Numbers/1", 10);                // the element that was at index 0
                jpd.Move("/Numbers/3", "/Last");              // from an array changed before
                jpd.Test("/NameCopy", "Rhinos");

                var converter = new ExpandoObjectConverter();
                dynamic expected = JsonConvert.DeserializeObject<ExpandoObject>(JsonConvert.SerializeObject(original), converter);
                jpd.ApplyTo(expected);

                Assert.Equal(PatchStatus.Patched, store.Operations.Send(new JsonPatchOperation(documentId, jpd)).Status);

                using (var session = store.OpenSession())
                {
                    dynamic stored = session.Load<ExpandoObject>(documentId);
                    AssertExpandosEqual(expected, stored);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void RemovingAPropertyAddedByThePatch()
        {
            dynamic original = new ExpandoObject();
            original.Name = "Hibernating";

            using (var store = GetDocumentStore())
            {
                string documentId;
                using (var session = store.OpenSession())
                {
                    session.Store(original);
                    session.SaveChanges();
                    documentId = original.Id;
                }

                var jpd = new JsonPatchDocument();
                jpd.Add("/Tag", "new");
                jpd.Replace("/Tag", "newer");
                jpd.Remove("/Tag");
                jpd.Replace("/Name", "Rhinos");

                var converter = new ExpandoObjectConverter();
                dynamic expected = JsonConvert.DeserializeObject<ExpandoObject>(JsonConvert.SerializeObject(original), converter);
                jpd.ApplyTo(expected);

                Assert.Equal(PatchStatus.Patched, store.Operations.Send(new JsonPatchOperation(documentId, jpd)).Status);

                using (var session = store.OpenSession())
                {
                    dynamic stored = session.Load<ExpandoObject>(documentId);
                    AssertExpandosEqual(expected, stored);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void ChangingTheSourceOfACopyDoesNotChangeTheCopy()
        {
            dynamic address = new ExpandoObject();
            address.City = "Netanya";
            dynamic original = new ExpandoObject();
            original.Address = address;
            original.Numbers = new List<int> { 1, 2 };

            using (var store = GetDocumentStore())
            {
                string documentId;
                using (var session = store.OpenSession())
                {
                    session.Store(original);
                    session.SaveChanges();
                    documentId = original.Id;
                }

                dynamic shipping = new ExpandoObject();
                shipping.City = "Haifa";

                var jpd = new JsonPatchDocument();
                jpd.Copy("/Address", "/Billing");
                jpd.Replace("/Address/City", "Hadera");
                jpd.Add("/Shipping", shipping);
                jpd.Copy("/Shipping", "/Return");
                jpd.Replace("/Return/City", "Eilat");
                jpd.Copy("/Numbers", "/MoreNumbers");
                jpd.Add("/Numbers/-", 3);

                var converter = new ExpandoObjectConverter();
                dynamic expected = JsonConvert.DeserializeObject<ExpandoObject>(JsonConvert.SerializeObject(original), converter);
                jpd.ApplyTo(expected);

                Assert.Equal(PatchStatus.Patched, store.Operations.Send(new JsonPatchOperation(documentId, jpd)).Status);

                using (var session = store.OpenSession())
                {
                    dynamic stored = session.Load<ExpandoObject>(documentId);
                    AssertExpandosEqual(expected, stored);
                    Assert.Equal("Netanya", (string)stored.Billing.City);
                    Assert.Equal("Haifa", (string)stored.Shipping.City);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public void TestingAnObjectChangedByThePatch()
        {
            using (var store = GetDocumentStore())
            {
                const string documentId = "people/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Person { Address = new SinglePropClass { City = "Netanya" } }, documentId);
                    session.SaveChanges();
                }

                var jpd = new JsonPatchDocument();
                jpd.Replace("/Address/City", "Hadera");
                jpd.Test("/Address", new SinglePropClass { City = "Netanya" });

                var error = Assert.ThrowsAny<RavenException>(() => store.Operations.Send(new JsonPatchOperation(documentId, jpd)));
                Assert.Contains("is not equal to the test value", error.Message);

                jpd = new JsonPatchDocument();
                jpd.Replace("/Address/City", "Hadera");
                jpd.Test("/Address", new SinglePropClass { City = "Hadera" });

                Assert.Equal(PatchStatus.Patched, store.Operations.Send(new JsonPatchOperation(documentId, jpd)).Status);
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public async Task RunningTheOperationsAgainGivesTheSameDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new MyTestClass { Name = "Hibernating" }, "items/1");
                    session.SaveChanges();
                }

                var database = await GetDatabase(store.Database);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var patch = context.ReadObject(new DynamicJsonValue
                    {
                        ["Operations"] = new DynamicJsonArray
                        {
                            new DynamicJsonValue { ["op"] = "add", ["path"] = "/MyArray", ["value"] = new DynamicJsonArray { 1, 2 } },
                            new DynamicJsonValue { ["op"] = "add", ["path"] = "/MyArray/-", ["value"] = 3 }
                        }
                    }, "patch");
                    var commands = JsonPatchCommand.Parse(patch);

                    // the merger runs a command again when the transaction it was merged into fails
                    await database.TxMerger.Enqueue(new JsonPatchCommand("items/1", commands, returnDocument: false, context));
                    await database.TxMerger.Enqueue(new JsonPatchCommand("items/1", commands, returnDocument: false, context));
                }

                using (var session = store.OpenSession())
                {
                    var stored = session.Load<MyTestClass>("items/1");
                    Assert.Equal(new List<int> { 1, 2, 3 }, stored.MyArray);
                }
            }
        }

        private class Person
        {
            public SinglePropClass Address;
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
