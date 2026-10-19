using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server
{
    public class WindowsServiceTests : NoDisposalNeeded
    {
        public WindowsServiceTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Windows, RavenPlatform.Windows)]
        public void CanAcceptPreshutdownNotifications()
        {
            // RavenWin32Service registers for preshutdown notifications through a private ServiceBase field,
            // this fails when a .NET update renames or changes it (the service would fall back to the much shorter shutdown notification)
            var field = RavenWin32Service.AcceptedCommandsField;

            Assert.NotNull(field);
            Assert.Equal(typeof(int), field.FieldType);
        }
    }
}
