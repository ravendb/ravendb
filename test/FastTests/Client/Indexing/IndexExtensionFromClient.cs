using System;
using System.Collections.Generic;
using System.Linq;
using NodaTime;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class IndexExtensionFromClient : RavenTestBase
    {
        [Fact]
        public void CanCompileIndexWithExtensions()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new PeopleByEmail());
                using (var session = store.OpenSession())
                {
                    var p = new Person() {Name = "Methuselah", Age = 969};
                    session.Store(p);
                    session.SaveChanges();
                    WaitForIndexing(store);
                    WaitForUserToContinueTheTest(store);
                    var query = session.Query<PeopleByEmail.PeopleByEmailResult, PeopleByEmail>()
                        .Where(x => x.Email == PeopleUtil.CalculatePersonEmail(p.Name, p.Age)).OfType<Person>().Single();
                }
            }
        }

        public class Person
        {
            public string Name { get; set; }
            public uint Age { get; set; }
        }

        public class PeopleByEmail : AbstractIndexCreationTask<Person>
        {
            public class PeopleByEmailResult
            {
                public string Email { get; set; }
            }

            public PeopleByEmail()
            {
                Map = people => from person in people select new
                {
                    _ =  CreateField("Email", PeopleUtil.CalculatePersonEmail(person.Name, person.Age), true, true),
                };
                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "PeopleUtil",
                        @"
using System;
using NodaTime;
namespace My.Crazy.Namespace
{
    public static class PeopleUtil
    {
        public static string CalculatePersonEmail(string name, uint age)
        {
            return $""{name}.{Instant.FromDateTimeUtc(DateTime.Now.ToUniversalTime()).Minus(Duration.FromDays(356*age)).ToDateTimeUtc().Year}@ayende.com"";
        }
    }
}
"
                    }
                };
            }
        }
    }

    public static class PeopleUtil
    {
        public static string CalculatePersonEmail(string name, uint age)
        {
            //The code below intention is just to make sure NodaTime is compiling with our index
            return $"{name}.{Instant.FromDateTimeUtc(DateTime.Now.ToUniversalTime()).ToDateTimeUtc().Year - age}@ayende.com";
        }
    }
}
