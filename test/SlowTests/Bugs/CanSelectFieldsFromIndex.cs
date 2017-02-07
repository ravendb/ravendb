//-----------------------------------------------------------------------
// <copyright file="CanSelectFieldsFromIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading;
using FastTests;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Indexing;
using Raven.NewClient.Operations.Databases.Indexes;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Bugs
{
    public class CanSelectFieldsFromIndex : RavenNewTestBase
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

                store.Commands().Put("ayende", null, myObject, new Dictionary<string, string>());

                var fieldOptions = new IndexFieldOptions {Storage = FieldStorage.Yes};

                store.Admin.Send(new PutIndexOperation("EmailAndProject", new IndexDefinition
                                                                          {
                                                                              Maps = { "from doc in docs from project in doc.projects select new {doc.email, doc.name, project };" },
                                                                              Fields =
                                                                              {
                                                                                {"email" , fieldOptions },
                                                                                {"name" , fieldOptions },
                                                                                {"project" , fieldOptions },
                                                                              }}));


                while (store.Commands().Query("EmailAndProject", new IndexQuery(new DocumentConvention())).IsStale)
                    Thread.Sleep(100);

                var queryResult = store.Commands().Query("EmailAndProject", new IndexQuery(new DocumentConvention())
                {
                    FieldsToFetch = new [] {"email"}
                });

                Assert.Equal(9, queryResult.Results.Length);
                
                foreach (BlittableJsonReaderObject result in queryResult.Results)
                {
                    Assert.Equal("ayende@ayende.com", result["email"].ToString());
                }
            }
        }
    }
}
