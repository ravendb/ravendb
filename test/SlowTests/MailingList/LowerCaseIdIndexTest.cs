using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class LowerCaseIdIndexTest : RavenTestBase
    {
        [Fact]
        public void CanIndexAndQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.FindIdentityProperty = q => q.Name == "id";

                var index = new UserToResource_Index();
                index.Execute(store.DatabaseCommands, store.Conventions);

                using (var session = store.OpenSession())
                {
                    var r1 = new Resource
                    {
                        id = "r1",
                        Name = "Car",
                        ResourceGroups = new List<DenormalizedReference>()
                        {
                            new DenormalizedReference(){ id="rg1", Name="RG1" }
                        }
                    };
                    session.Store(r1);

                    var r2 = new Resource
                    {
                        id = "r2",
                        Name = "Truck",
                        ResourceGroups = new List<DenormalizedReference>()
                        {
                            new DenormalizedReference(){ id="rg1", Name="RG1" }
                        }
                    };
                    session.Store(r2);

                    session.Store(new ResourceGroup
                    {
                        id = "rg1",
                        Name = "RG1",
                        Resources = new List<DenormalizedReference>()
                        {
                            new DenormalizedReference(){ id=r1.id, Name=r1.Name },
                            new DenormalizedReference(){ id=r2.id, Name=r2.Name }
                        },
                        ResourceUserGroups = new List<DenormalizedReference>()
                        {
                            new DenormalizedReference(){ id="rug1", Name="RUG1" },
                        },
                    });

                    session.Store(new ResourceUserGroup
                    {
                        id = "rug1",
                        Name = "RUG1",
                        ResourceGroups = new List<DenormalizedReference>()
                        {
                            new DenormalizedReference(){ id="rg1", Name="RG1" }
                        },
                        Users = new List<DenormalizedReference>()
                        {
                            new DenormalizedReference(){ id="u1", Name="Tester" }
                        }
                    });

                    var u1 = new User
                    {
                        id = "u1",
                        Name = "Tester",
                        ResourceUserGroups = new List<DenormalizedReference>()
                        {
                            new DenormalizedReference(){ id="rug1", Name="RUG1" }
                        }
                    };
                    session.Store(u1);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var firstTest = session.Advanced.DocumentQuery<UserToResource_Index.ResourceToUserIndexData, UserToResource_Index>()
                            .WaitForNonStaleResults()
                            .WhereEquals("Name", "Tester")
                            .FirstOrDefault();

                    Assert.NotNull(firstTest);
                    Assert.NotNull(firstTest.UserId);

                    var secondTest = session.Advanced.DocumentQuery<UserToResource_Index.ResourceToUserIndexData, UserToResource_Index>()
                             .WhereEquals("UserId", firstTest.UserId)
                             .FirstOrDefault();

                    Assert.NotNull(secondTest);
                }
            }
        }

        private class Resource
        {
            public string id { get; set; }
            public string Name { get; set; }
            public List<DenormalizedReference> ResourceGroups { get; set; }
        }

        private class ResourceGroup
        {
            public string id { get; set; }
            public string Name { get; set; }
            public List<DenormalizedReference> Resources { get; set; }
            public List<DenormalizedReference> ResourceUserGroups { get; set; }
        }

        private class ResourceUserGroup
        {
            public string id { get; set; }
            public string Name { get; set; }
            public List<DenormalizedReference> ResourceGroups { get; set; }
            public List<DenormalizedReference> Users { get; set; }
        }

        private class User
        {
            public string id { get; set; } //Id is ok
            public string Name { get; set; }
            public List<DenormalizedReference> ResourceUserGroups { get; set; }
        }

        private class DenormalizedReference
        {
            public string id { get; set; }
            public string Name { get; set; }
        }

        private class UserToResource_Index : AbstractIndexCreationTask<User, UserToResource_Index.ResourceToUserIndexData>
        {
            public class ResourceToUserIndexData
            {
                public string UserId { get; set; }
                public string Name { get; set; }
                public IEnumerable<string> Resources { get; set; }
            }


            //Map user to Resources 
            //User -> 1 or more resourceUserGroups -> 1 or more resourceGroups -> 1 or more Resources
            public UserToResource_Index()
            {
                //Map = Usr => Usr.SelectMany(u => Enumerable.Select(u.ResourceUserGroups, x => LoadDocument<ResourceUserGroup>(x.id).ResourceGroups), (u, rgs) => new {u, rgs})
                //	.SelectMany(@t => Enumerable.Select(@t.rgs, y => LoadDocument<ResourceGroup>(y.id).Resources), (@t, rs) => new ResourceToUserIndexData
                //{
                //	UserId = @t.u.id,
                //	Name = @t.u.Name,
                //	Resources = rs.Select(r => r.id).GroupBy(x => x).Select(x => x.Key)
                //});


                Map = Usr => from u in Usr
                             from rgs in Enumerable.Select<DenormalizedReference, List<DenormalizedReference>>(u.ResourceUserGroups, x => LoadDocument<ResourceUserGroup>(x.id).ResourceGroups)
                             from rs in Enumerable.Select<DenormalizedReference, List<DenormalizedReference>>(rgs, y => LoadDocument<ResourceGroup>(y.id).Resources)
                             select new ResourceToUserIndexData
                             {
                                 UserId = u.id,
                                 Name = u.Name,
                                 Resources = rs.Select(r => r.id).GroupBy(x => x).Select(x => x.Key)
                             };

                //reducing... group by UserId to remove duplicates
                Reduce = docs => from doc in docs
                                 group doc by doc.UserId into g
                                 select new
                                 {
                                     UserId = g.Key,
                                     Name = g.Select(x => x.Name).First(),
                                     Resources = g.SelectMany(x => x.Resources).GroupBy(x => x).Select(x => x.Key),
                                 };
            }
        }

    }
}
