using log4net.Appender;
using log4net.Config;
using log4net.Layout;

namespace Raven.Tests.Storage
{
    public class WithDebugging
    {
        static WithDebugging()
        {
            BasicConfigurator.Configure(
                new OutputDebugStringAppender()
                {
                    Layout = new SimpleLayout()
                });
        }
    }
}