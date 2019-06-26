using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using NodaTime;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using static FastTests.Client.Indexing.PeopleUtil;

namespace FastTests.Client.Indexing
{
    public class IndexExtensionFromClient : RavenTestBase
    {
        [Fact]
        public void CanCompileIndexWithExtensions()
        {
            CopyNodaTimeIfNeeded();

            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleByEmail());
                using (var session = store.OpenSession())
                {
                    var p = new Person() { Name = "Methuselah", Age = 969 };
                    session.Store(p);
                    session.SaveChanges();
                    WaitForIndexing(store);
                    var query = session.Query<PeopleByEmail.PeopleByEmailResult, PeopleByEmail>()
                        .Where(x => x.Email == PeopleUtil.CalculatePersonEmail(p.Name, p.Age)).OfType<Person>().Single();
                }
            }
        }

        [Fact]
        public async Task CanUpdateIndexExtensions()
        {
            using (var store = GetDocumentStore())
            {
                var getRealCountry = @"
using System.Globalization;
namespace My.Crazy.Namespace
{
    public static class Helper
    {
        public static string GetRealCountry(string name)
        {
            return new RegionInfo(name).EnglishName;
        }
    }
}
";

                await store.ExecuteIndexAsync(new RealCountry(getRealCountry));

                var additionalSources = await GetAdditionalSources();
                Assert.Equal(1, additionalSources.Count);
                Assert.Equal(getRealCountry, additionalSources["Helper"]);

                getRealCountry = getRealCountry.Replace(".EnglishName", ".Name");
                store.ExecuteIndex(new RealCountry(getRealCountry));

                additionalSources = await GetAdditionalSources();
                Assert.Equal(1, additionalSources.Count);
                Assert.Equal(getRealCountry, additionalSources["Helper"]);

                async Task<Dictionary<string, string>> GetAdditionalSources()
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Indexes.First().Value.AdditionalSources;
                }
            }
        }

        [Fact]
        public void CanUseMethodFromExtensionsInIndex_List()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex1());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        Friends = new List<string>
                        {
                            "jerry", "bob", "ayende"
                        }
                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        Friends = new List<string>
                        {
                            "david", "ayende"
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex1>().Single();
                    Assert.Equal("aviv", person.Name);
                }
            }
        }

        [Fact]
        public void CanUseMethodFromExtensionsInIndex_Dictionary()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex2());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        Contacts = new Dictionary<string, long>
                        {
                            {
                                "jerry", 5554866812
                            },
                            {
                                "bob", 5554866813
                            },
                            {
                                "ayende", 5554866814
                            }
                        }

                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        Contacts = new Dictionary<string, long>
                        {
                            {
                                "david", 5554866815
                            },
                            {
                                "ayende", 5554866814
                            }
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex2>().Single();
                    Assert.Equal("aviv", person.Name);
                }
            }
        }

        [Fact]
        public void CanUseMethodFromExtensionsInIndex_ICollection()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex3());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        FriendsCollection = new List<string>
                        {
                            "jerry",
                            "bob"
                        }

                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        FriendsCollection = new List<string>
                        {
                            "jerry",
                            "david",
                            "ayende"
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex3>().Single();
                    Assert.Equal("aviv", person.Name);
                }
            }
        }

        [Fact]
        public void CanUseMethodFromExtensionsInIndex_Hashset()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex4());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        FriendsHashset = new HashSet<string>
                        {
                            "jerry",
                            "bob"
                        }

                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        FriendsHashset = new HashSet<string>
                        {
                            "jerry",
                            "david",
                            "ayende"
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex4>().Single();
                    Assert.Equal("aviv", person.Name);
                }
            }
        }

        [Fact]
        public void CanUseMethodFromExtensionsInIndex_ListOfUsers()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex5());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        UserFriends = new List<User>
                        {
                            new User
                            {
                                Name = "jerry"
                            },
                            new User
                            {
                                Name = "bob"
                            }
                        }

                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        UserFriends = new List<User>
                        {
                            new User
                            {
                                Name = "david"
                            },
                            new User
                            {
                                Name = "ayende"
                            }
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex5>().Single();
                    Assert.Equal("aviv", person.Name);
                }
            }
        }

        [Fact]
        public void CanUseMethodFromExtensionsInIndex_Array()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex6());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        FriendsArray = new[]
                        {
                            new User
                            {
                                Name = "jerry"
                            },
                            new User
                            {
                                Name = "bob"
                            }
                        }

                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        FriendsArray = new[]
                        {
                            new User
                            {
                                Name = "david"
                            },
                            new User
                            {
                                Name = "ayende"
                            }
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex6>().Single();
                    Assert.Equal("aviv", person.Name);
                }
            }
        }

        [Fact]
        public void CanUseMethodFromExtensionsInIndex_MyList()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex7());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        MyList = new MyList<string>
                        {
                            "julian", "ricky", "ayende"
                        }
                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        MyList = new MyList<string>
                        {
                            "david", "ayende"
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex7>().Single();
                    Assert.Equal("aviv", person.Name);
                }
            }
        }

        [Fact]
        public void CanUseMethodFromExtensionsInIndex_MyEnumerable()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex8());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        MyEnumerable = new MyEnumerable<string>(new List<string>
                        {
                            "julian", "ricky", "ayende"
                        })
                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        MyEnumerable = new MyEnumerable<string>(new List<string>
                        {
                            "david", "ayende"
                        })
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex8>().Single();
                    Assert.Equal("aviv", person.Name);
                }
            }
        }

        [Fact]
        public void CanUseMethodFromExtensionsInIndex_DateTime()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex9());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        Event = new Event
                        {
                            StartTime = new DateTime(2018, 1, 1),
                            EndTime = new DateTime(2020, 1, 1)
                        }
                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        Event = new Event
                        {
                            StartTime = new DateTime(2018, 1, 1),
                            EndTime = new DateTime(2018, 2, 1)
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex9>().Single();
                    Assert.Equal("reeb", person.Name);
                }
            }
        }

        [Fact(Skip = "need to use DynamicDictionary. waiting for PR from Egor")]
        public void CanUseMethodFromExtensionsInIndex_DictionaryFunctions()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleIndex10());

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "aviv",
                        Contacts = new Dictionary<string, long>
                        {
                            {
                                "jerry", 5554866812
                            },
                            {
                                "bob", 5554866813
                            },
                            {
                                "ayende", 5554866814
                            }
                        }

                    });

                    session.Store(new Person
                    {
                        Name = "reeb",
                        Contacts = new Dictionary<string, long>
                        {
                            {
                                "david", 5554866815
                            },
                            {
                                "ayende", 5554866814
                            },
                            {
                                "home", 1024
                            }
                        }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var person = session.Query<Person, PeopleIndex10>().Single();
                    Assert.Equal("reeb", person.Name);
                }
            }
        }

        private class RealCountry : AbstractIndexCreationTask<Order>
        {
            public RealCountry(string getRealCountry)
            {
                Map = orders => from order in orders
                    select new
                    {
                        Country = Helper.GetRealCountry(order.ShipTo.Country)
                    };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "Helper",
                        getRealCountry
                    }
                };
            }

            private static class Helper
            {
                public static string GetRealCountry(string name)
                {
                    return new RegionInfo(name).EnglishName;
                }
            }
        }

        private static void CopyNodaTimeIfNeeded()
        {
            var nodaLocation = new FileInfo(typeof(Instant).Assembly.Location);
            var currentLocation = new FileInfo(typeof(IndexExtensionFromClient).Assembly.Location);
            var newLocation = new FileInfo(Path.Combine(currentLocation.DirectoryName, nodaLocation.Name));
            if (newLocation.Exists)
                return;

            File.Copy(nodaLocation.FullName, newLocation.FullName, overwrite: true);
        }

        private class Person
        {
            public string Name { get; set; }

            public uint Age { get; set; }

            public List<string> Friends { get; set; }

            public Dictionary<string, long> Contacts { get; set; }

            public ICollection<string> FriendsCollection { get; set; }

            public HashSet<string> FriendsHashset { get; set; }

            public IEnumerable<User> UserFriends { get; set; }

            public User[] FriendsArray { get; set; }

            public MyList<string> MyList { get; set; }

            public MyEnumerable<string> MyEnumerable { get; set; }

            public Event Event { get; set; }

        }

        private class Event
        {
            public DateTime StartTime { get; set; }
            public DateTime? EndTime { get; set; }

        }

        private class PeopleByEmail : AbstractIndexCreationTask<Person>
        {
            public class PeopleByEmailResult
            {
                public string Email { get; set; }
            }

            public PeopleByEmail()
            {
                Map = people => from person in people
                    select new
                    {
                        _ = CreateField("Email", CalculatePersonEmail(person.Name, person.Age), true, true),
                    };
                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System;
using NodaTime;
using static My.Crazy.Namespace.PeopleUtil;
namespace My.Crazy.Namespace
{
    public static class PeopleUtil
    {
        public static string CalculatePersonEmail(string name, uint age)
        {
            //The code below intention is just to make sure NodaTime is compiling with our index
            return $""{name}.{Instant.FromDateTimeUtc(DateTime.Now.ToUniversalTime()).ToDateTimeUtc().Year - age}@ayende.com"";
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex1 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex1()
            {
                Map = people => from person in people
                                where Foo1(person.Friends)
                                select new
                                {
                                    person.Name
                                };


                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System.Collections.Generic;
using System.Linq;
namespace ETIS
{
    public static class PeopleUtil
    {
        public static bool Foo1(List<string> friends)
        {
            return friends.Count(n => n != ""ayende"") > 1;
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex2 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex2()
            {
                Map = people => from person in people
                                where Foo2(person.Contacts)
                                select new
                                {
                                    person.Name
                                };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System.Collections.Generic;
using System.Linq;
namespace ETIS
{
    public static class PeopleUtil
    {
        public static bool Foo2(Dictionary<string, long> friends)
        {
            return friends.Count(n => n.Key != ""ayende"") > 1;
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex3 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex3()
            {
                Map = people => from person in people
                                where Foo3(person.FriendsCollection)
                                select new
                                {
                                    person.Name
                                };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System.Collections.Generic;
using System.Linq;
namespace ETIS
{
    public static class PeopleUtil
    {
         public static bool Foo3(ICollection<string> friends)
        {
            return friends.All(n => n != ""ayende"");
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex4 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex4()
            {
                Map = people => from person in people
                                where Foo4(person.FriendsHashset)
                                select new
                                {
                                    person.Name
                                };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System.Collections.Generic;
using System.Linq;
namespace ETIS
{
    public static class PeopleUtil
    {
        public static bool Foo4(HashSet<string> friends)
        {
            return friends.All(n => n != ""ayende"");
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex5 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex5()
            {
                Map = people => from person in people
                                where Foo5(person.UserFriends)
                                select new
                                {
                                    person.Name
                                };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System.Collections.Generic;
using System.Linq;
namespace ETIS
{
    public static class PeopleUtil
    {
        public class User
        {
            public string Name { get; set; }
        }

        public static bool Foo5(IEnumerable<User> friends)
        {
            return friends.All(n => n.Name != ""ayende"");
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex6 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex6()
            {
                Map = people => from person in people
                                where Foo6(person.FriendsArray)
                                select new
                                {
                                    person.Name
                                };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System.Collections.Generic;
using System.Linq;
namespace ETIS
{
    public static class PeopleUtil
    {
        public class User
        {
            public string Name { get; set; }
        }

        public static bool Foo6(User[] friends)
        {
            return friends.All(n => n.Name != ""ayende"");
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex7 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex7()
            {
                Map = people => from person in people
                    where Foo7(person.MyList)
                    select new
                    {
                        person.Name
                    };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System.Collections.Generic;
using System.Linq;
namespace ETIS
{
    public static class PeopleUtil
    {
        public class MyList<T> : List<T>
        {
        }

        public static bool Foo7(MyList<string> friends)
        {
            return friends.Count(n => n != ""ayende"") > 1;
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex8 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex8()
            {
                Map = people => from person in people
                    where Foo8(person.MyEnumerable)
                    select new
                    {
                        person.Name
                    };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System.Collections;
using System.Collections.Generic;
using System.Linq;
namespace ETIS
{
    public static class PeopleUtil
    {
        public class MyEnumerable<T> : IEnumerable<T>
        {
            private IEnumerable<T> _list;
            public MyEnumerable()
            {
            }

            public MyEnumerable(IEnumerable<T> list)
            {
                _list = list;
            }
            public IEnumerator<T> GetEnumerator()
            {
                return _list.GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        public static bool Foo8(MyEnumerable<string> friends)
        {
            return friends.Count(n => n != ""ayende"") > 1;
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex9 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex9()
            {
                Map = people => from person in people
                    where Foo9(person.Event.StartTime, person.Event.EndTime).TotalDays < 100
                    select new
                    {
                        person.Name
                    };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System;
namespace ETIS
{
    public static class PeopleUtil
    {
        public static TimeSpan Foo9(DateTime start, DateTime? end)
        {
            if (end.HasValue == false)
                return TimeSpan.MaxValue;
            return end.Value - start;
        }
    }
}
"
                    }
                };
            }
        }

        private class PeopleIndex10 : AbstractIndexCreationTask<Person>
        {
            public PeopleIndex10()
            {
                Map = people => from person in people
                    where Foo10(person.Contacts) > 100 
                    select new
                    {
                        person.Name
                    };

                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System.Collections.Generic;
using System.Linq;
namespace ETIS
{
    public static class PeopleUtil
    {
        public static long Foo10(Dictionary<string, long> friends)
        {
            if (friends == null)
                return -1;
            if (friends.ContainsKey(""home""))
                return 0;
            return friends.Values.Sum(x => x);
        }
    }
}
"
                    }
                };
            }
        }

    }

    public class MyList<T> : List<T>
    {

    }

    public class MyEnumerable<T> : IEnumerable<T>
    {
        private IEnumerable<T> _list;
        public MyEnumerable()
        {
            
        }

        public MyEnumerable(IEnumerable<T> list)
        {
            _list = list;
        }
        public IEnumerator<T> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    public static class PeopleUtil
    {
        public static string CalculatePersonEmail(string name, uint age)
        {
            //The code below intention is just to make sure NodaTime is compiling with our index
            return $"{name}.{Instant.FromDateTimeUtc(DateTime.Now.ToUniversalTime()).ToDateTimeUtc().Year - age}@ayende.com";
        }

        public static bool Foo1(List<string> friends)
        {
            return friends.Count(n => n != "ayende") > 1;
        }

        public static bool Foo2(Dictionary<string, long> friends)
        {
            return friends.Count(n => n.Key != "ayende") > 1;
        }

        public static bool Foo3(ICollection<string> friends)
        {
            return friends.All(n => n != "ayende");
        }

        public static bool Foo4(HashSet<string> friends)
        {
            return friends.All(n => n != "ayende");
        }

        public static bool Foo5(IEnumerable<User> friends)
        {
            return friends.All(n => n.Name != "ayende");
        }

        public static bool Foo6(User[] friends)
        {
            return friends.All(n => n.Name != "ayende");
        }

        public static bool Foo7(MyList<string> friends)
        {
            return friends.Count(n => n != "ayende") > 1;
        }

        public static bool Foo8(MyEnumerable<string> friends)
        {
            return friends.Count(n => n != "ayende") > 1;
        }

        public static TimeSpan Foo9(DateTime start, DateTime? end)
        {
            if (end.HasValue == false)
                return TimeSpan.MaxValue;
            return end.Value - start;
        }

        public static long Foo10(Dictionary<string, long> friends)
        {
            if (friends == null)
                return -1;
            if (friends.ContainsKey("home"))
                return 0;
            return friends.Values.Sum(x => x);
        }
    }
}
