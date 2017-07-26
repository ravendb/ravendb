using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace FastTests.Client.Indexing
{
    public class IndexExtensionFromClient : RavenTestBase
    {
        [Fact]
        public async Task CanCompileIndexWithExtensions()
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
                    var query = session.Query<PeopleByEmail.PeopleByEmailResult, PeopleByEmail>().Where(x=>x.Email == PeopleUtil.CalculatePersonEmail(p.Name,p.Age)).OfType<Person>().Single();
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
namespace My.Crazy.Namespace
{
    public static class PeopleUtil
    {
        public static string CalculatePersonEmail(string name, uint age)
        {
            return $""{name}.{DateTime.Now.Year - age}@ayende.com"";
        }
    }

    public class Person
    {
        public string Name { get; set; }
        public uint Age { get; set; }
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
            return $"{name}.{DateTime.Now.Year - age}@ayende.com";
        }
    }
}
