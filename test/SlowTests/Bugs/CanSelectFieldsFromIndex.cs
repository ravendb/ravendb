//-----------------------------------------------------------------------
// <copyright file="CanSelectFieldsFromIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Bugs
{
    public class CanSelectFieldsFromIndex : RavenTestBase
    {
        [Fact]
        public void SelectFieldsFromIndex()
        {
            using(var store = GetDocumentStore())
            {
                var myObject = new 
                {
                    name = "ayende" ,
                    email = "ayende@ayende.com",
                    projects = new  []
                    {
                        "rhino mocks",
                        "nhibernate",
                        "rhino service bus",
                        "rhino divan db",
                        "rhino persistent hash table",
                        "rhino distributed hash table",
                        "rhino etl",
                        "rhino security",
                        "rampaging rhinos"
                    }                    
                };

                store.Commands().Put("ayende", null, myObject);

                var fieldOptions = new IndexFieldOptions {Storage = FieldStorage.Yes};

                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                                                                          {
                                                                              Maps = { "from doc in docs from project in doc.projects select new {doc.email, doc.name, project };" },
                                                                              Name = "EmailAndProject",
                                                                              Fields =
                                                                              {
                                                                                {"email" , fieldOptions },
                                                                                {"name" , fieldOptions },
                                                                                {"project" , fieldOptions },
                                                                              }}}));


                while (store.Commands().Query(new IndexQuery { Query = "FROM INDEX 'EmailAndProject'" }).IsStale)
                    Thread.Sleep(100);

                var queryResult = store.Commands().Query(new IndexQuery { Query = "SELECT email FROM INDEX 'EmailAndProject'" });

                Assert.Equal(9, queryResult.Results.Length);
                
                foreach (BlittableJsonReaderObject result in queryResult.Results)
                {
                    Assert.Equal("ayende@ayende.com", result["email"].ToString());
                }
            }
        }
    }
}
