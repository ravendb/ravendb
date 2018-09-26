using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using static SlowTests.Issues.RavenDB_11985.Building;
using static SlowTests.Issues.RavenDB_11985.UsersIndexWithStoredArray;

namespace SlowTests.Issues
{    
    public class RavenDB_11985:RavenTestBase
    {
        public class Building
        {
            public class Tennant
            {
                public string FirstName;
                public string LastName;
                public int Income;
            }

            public string Address;
            public int Height;
            public IEnumerable<Tennant> Tennats;
        }
        public class UsersIndexWithStoredArray : AbstractIndexCreationTask<Building, UsersIndexWithStoredArray.IndexedBuilding>
        {            
            public class IndexedBuilding
            {
                public string Address;
                public IEnumerable<Tennant> StoredTennatns;
            }

            public UsersIndexWithStoredArray()
            {
                this.Map = buildings => from building in buildings
                                        select new IndexedBuilding
                                        {
                                            Address = building.Address,
                                            StoredTennatns = building.Tennats.Select(x => new Tennant
                                            {
                                                FirstName = x.FirstName,
                                                Income = x.Income
                                            })
                                        };
                Store(x => x.StoredTennatns, FieldStorage.Yes);

            }
        }
        [Fact]
        public void JSShouldReceiveValidStoredObjectsArray()
        {
            using (var store = GetDocumentStore())
            {
                new UsersIndexWithStoredArray().Execute(store);
                using (var session = store.OpenSession())
                {
                    for (var i=0; i< 10; i++)
                    {
                        session.Store(new Building
                        {
                            Address = "Somewhere " + i,
                            Height = i,
                            Tennats = Enumerable.Range(0, 5).Select(x => new Tennant
                            {
                                FirstName = "Jane" + i,
                                LastName = "Doe" + i,
                                Income = i

                            })

                        });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {                    
                    var results = session.Query<IndexedBuilding, UsersIndexWithStoredArray>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .Select(x => new IndexedBuilding
                        {
                            Address = x.Address + "22",
                            StoredTennatns = x.StoredTennatns
                        }).ToList();

                    Assert.Equal(10, results.Count);
                    Assert.Equal(5, results[0].StoredTennatns.Count());
                }
            }            
        }


        [Fact]
        public void MakeSureStoredInexedValueIsNotStoredInDocumentDuringPatchOperation()
        {
            using (var store = GetDocumentStore())
            {
                new UsersIndexWithStoredArray().Execute(store);
                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new Building
                        {
                            Address = "Somewhere " + i,
                            Height = i,
                            Tennats = Enumerable.Range(0, 5).Select(x => new Tennant
                            {
                                FirstName = "Jane" + i,
                                LastName = "Doe" + i,
                                Income = i

                            })

                        });
                    }

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<IndexedBuilding, UsersIndexWithStoredArray>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Count();
                    Assert.Equal(10, results);
                }

                var operation = store
                    .Operations
                    .Send(new PatchByQueryOperation(new IndexQuery
                    {
                        Query = @"from index 'UsersIndexWithStoredArray' as p                  
                                  update
                                  {
                                        if (!!p.StoredTennatns)
                                        {
                                            p.Height++;
                                        }                                      
                                  }"
                    }));

                operation.WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var building = session.Query<Building>().OfType<BlittableJsonReaderObject>().FirstOrDefault();

                    Assert.False(building.TryGet<BlittableJsonReaderArray>("StoredTennatns", out var o));
                }
            }
        }
    }
}
