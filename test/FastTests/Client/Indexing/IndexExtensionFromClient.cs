using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using NodaTime;
using Raven.Client.Documents.Indexes;
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
