using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Principal;

using Xunit.Extensions;
using Xunit.Sdk;

namespace Raven.Tests.Common.Attributes
{
	[CLSCompliant(false)]
	public class AdminOnlyWithIIS7Installed : TheoryAttribute
	{
		protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
		{
			var displayName = method.TypeName + "." + method.Name;

			if (File.Exists(@"C:\Windows\System32\InetSrv\Microsoft.Web.Administration.dll") == false)
			{
				yield return
					new SkipCommand(method, displayName,
						"Could not execute " + displayName + " because it requires IIS7 and could not find Microsoft.Web.Administration");
				yield break;
			}

			var windowsIdentity = WindowsIdentity.GetCurrent();
			if (windowsIdentity != null)
			{
				if (new WindowsPrincipal(windowsIdentity).IsInRole(WindowsBuiltInRole.Administrator) == false)
				{
					yield return
						new SkipCommand(method, displayName,
							"Could not execute " + displayName + " because it requires Admin privileges");
					yield break;
				}
			}

			foreach (var command in base.EnumerateTestCommands(method))
			{
				yield return command;
			}
		}
	}
}