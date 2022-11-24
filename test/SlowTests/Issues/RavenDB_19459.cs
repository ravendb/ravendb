using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{

    public class RavenDB_19459 : RavenTestBase
    {
        public RavenDB_19459(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DictionaryAsReturnTypeInAdditionalSourcesWillNotBeRewrittenAsExtensionOfArgument()
        {
            using var store = GetDocumentStore();
            {
                using var s = store.OpenSession();
                s.Store(new People() {Dict = "maciej"});
                s.SaveChanges();
            }
            var index = new PeopleByEmail();
            index.Execute(store);
            Indexes.WaitForIndexing(store);
            {
                using var s = store.OpenSession();
                var query = s.Query<People, PeopleByEmail>().Where(i => i.Dict == "Maciej").ToList();
                Assert.Equal(1, query.Count);
                Assert.Equal(query.First().Dict, "maciej");
            }
            
        }

        private class People
        {
            public string Dict { get; set; }
        }

        private class PeopleByEmail : AbstractIndexCreationTask<People>
        {


            public PeopleByEmail()
            {
                Map = people => from person in people
                    select new {Dict = My.Crazy.Namespace.Util.CalculateDictionary(person.Dict).Select(i => i.Value),};
                AdditionalSources = new Dictionary<string, string>
                {
                    {
                        "Util", @"
using System;
using NodaTime;
using static My.Crazy.Namespace.Util;
using System.Collections.Generic;
using System;

namespace My.Crazy.Namespace
{
    public static class Util
    {
        public static Dictionary<string, string> CalculateDictionary(string name)
        {
            return new Dictionary<string, string>(){{name, name}};
        }
    }
}
"
                    }
                };
            }
        }
    }
}
namespace My.Crazy.Namespace
{
    public static class Util
    {
        public static Dictionary<string, string> CalculateDictionary(string name)
        {
            return new Dictionary<string, string>(){{"maciej", name}};
        }
    }
}
