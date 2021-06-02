using FastTests;
using Jint;
using Jint.Native;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

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
            var engine = new Engine(options =>
            {
                options.Strict();
            });

            var args = new JsValue[1];
            var user = new User();
            args[0] = JsValue.FromObject(engine, user);

            engine.Execute(script);
            var call = engine.GetValue("execute").TryCast<ICallable>();
            call.Call(Undefined.Instance, args);

            Assert.Equal("ayende", user.Name);
        }
    }
}
