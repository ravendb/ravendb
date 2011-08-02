// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System.Collections.Generic;
using System.Security.Principal;
using Xunit;
using Xunit.Extensions;
using Xunit.Sdk;

namespace Raven.Tests.Bugs.Identifiers
{
	public class SpecialCharactersOnIIS : WithNLog
	{
		[AdminOnlyTheory]
		[InlineData("foo")]
		[InlineData("SHA1-UdVhzPmv0o+wUez+Jirt0OFBcUY=")]
		public void Can_load_entity(string entityId)
		{
			var testContext = new IISClientTest();

			using (var store = testContext.GetDocumentStore())
			{
				store.Initialize();

				using (var session = store.OpenSession())
				{
					var entity = new Entity {Id = entityId};
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

		#region Nested type: Entity

		public class Entity
		{
			public string Id { get; set; }
		}

		#endregion
	}

	public class AdminOnlyTheory : TheoryAttribute
	{
		protected override IEnumerable<ITestCommand> EnumerateTestCommands(IMethodInfo method)
		{
			var windowsIdentity = WindowsIdentity.GetCurrent();
			if (windowsIdentity != null)
			{
				if (new WindowsPrincipal(windowsIdentity).IsInRole(WindowsBuiltInRole.Administrator) == false)
				{
					var displayName = method.TypeName + "." + method.Name;
					yield return
						new SkipCommand(method, displayName,
						                "Could not execute " + displayName +" because it requires Admin privileges");
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