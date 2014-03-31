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

			if (File.Exists(@"c:\Program Files (x86)\IIS Express\iisexpress.exe") == false && File.Exists(@"c:\Program Files\IIS Express\iisexpress.exe") == false)
			{
				yield return
					new SkipCommand(method, displayName,
						"Could not execute " + displayName + " because it requires IIS Express and could not find it at c:\\Program Files (x86)\\. or at c:\\Program Files\\.   Considering installing the MSI from http://www.microsoft.com/download/en/details.aspx?id=1038");
				yield break;
			}

			foreach (var command in base.EnumerateTestCommands(method))
			{
				yield return command;
			}
		}
	}
}