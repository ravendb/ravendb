using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13178 : RavenTestBase
    {
        public RavenDB_13178(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CompilationOfJavaScriptIndexShouldNotTakeIntoAccountTheMaxStepsForScript()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.MaxStepsForScript)] = "1"
            }))
            {
                new UsersByPhones().Execute(store);
            }
        }

        private class UsersByPhones : AbstractJavaScriptIndexCreationTask
        {
            public class UsersByPhonesResult
            {
                public string Name { get; set; }
                public string Phone { get; set; }
            }

            public UsersByPhones()
            {
                Maps = new HashSet<string>
                {
                    @"map('Users', function (u){ return { Name: u.Name, Phone: u.PhoneNumbers};})",
                };
            }
        }
    }
}
