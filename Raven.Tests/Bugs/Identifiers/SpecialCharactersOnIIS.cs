// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Security.Principal;
using Raven.Client.Document;
using Xunit;
using Xunit.Extensions;
using Xunit.Sdk;

namespace Raven.Tests.Bugs.Identifiers
{
	[CLSCompliant(false)]
	public class SpecialCharactersOnIIS : WithNLog
	{
		[IISExpressInstalledTheory]
		[InlineData("foo")]
		[InlineData("SHA1-UdVhzPmv0o+wUez+Jirt0OFBcUY=")]
		public void Can_load_entity(string entityId)
		{
			using (var testContext = new IisExpressTestClient())
			{
				using (var store = testContext.NewDocumentStore())
				{
					store.Initialize();

					using (var session = store.OpenSession())
					{
						var entity = new WithBase64Characters.Entity { Id = entityId };
						session.Store(entity);
						session.SaveChanges();
					}

					using (var session = store.OpenSession())
					{
						var entity1 = session.Load<object>(entityId);
						Assert.NotNull(entity1);
					}
				}
			}
		}

		#region Nested type: Entity

		public class Entity
		{
			public string Id { get; set; }
		}

		#endregion
	}

	[CLSCompliant(false)]
	public class IISExpressInstalledTheoryAttribute : TheoryAttribute
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