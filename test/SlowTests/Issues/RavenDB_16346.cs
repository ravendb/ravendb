using FastTests;
using V8.Net;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;
using Xunit.Abstractions;
using Raven.Server.Documents.Patch;

namespace SlowTests.Issues
{
    public class RavenDB_16346 : RavenTestBase
    {
        public RavenDB_16346(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            var script = @"
function execute(doc, args)
{
    var i = doc;
    {
        function doSomething() {
            return ""ayende"";
        }

        i.Name = doSomething();
    }
}
";
            var engine = new V8EngineEx();
            string[] optionsCmd = {$"use_strict={configuration.Patching.StrictMode}"};
            engine.SetFlagsFromCommandLine(optionsCmd);

            var args = new InternalHandle[1];
            var user = new User();
            using (args[0] = engine.FromObject(user))
            {
                engine.Execute(script);
                using(var execute = engine.GlobalObject.GetProperty("execute"))
                {
                    var call = execute.Object.TryCast<V8Function>();
                    call.StaticCall(args);
                }
            }

            Assert.Equal("ayende", user.Name);
        }
    }
}
