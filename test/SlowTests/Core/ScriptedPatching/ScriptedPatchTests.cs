using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Core.ScriptedPatching
{
    public class ScriptedPatchTests : RavenTestBase
    {
        public class Foo
        {
            public string Id { get; set; }

            public string BarId { get; set; }

            public string FirstName { get; set; }

            public string Fullname { get; set; }
        }

        public class Bar
        {
            public string Id { get; set; }

            public string LastName { get; set; }
        }
    }
}
