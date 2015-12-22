using System;
using System.Collections.Generic;
using System.IO;

using Xunit;
using Xunit.Sdk;

namespace Raven.Tests.Common.Attributes
{
    [CLSCompliant(false)]
    public class IISExpressInstalledFactAttribute : FactAttribute
    {
        protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
        {
            var displayName = method.TypeName + "." + method.Name;

            if (File.Exists(@"c:\Program Files\IIS Express\iisexpress.exe") == false)
            {
                yield return
                    new SkipCommand(method, displayName,
                        "Could not execute " + displayName + " because it requires IIS Express and could not find it at c:\\Program Files (x86)\\. or at c:\\Program Files\\. " +
                        "Considering installing the MSI from https://www.microsoft.com/en-us/download/details.aspx?id=48264 (IIS 10.0) http://www.microsoft.com/en-us/download/details.aspx?id=34679 (IIS 8.0) or http://www.microsoft.com/download/en/details.aspx?id=1038 (IIS 7.5)");
                yield break;
            }

            foreach (var command in base.EnumerateTestCommands(method))
            {
                yield return command;
            }
        }
    }
}
