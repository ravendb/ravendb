// //-----------------------------------------------------------------------
// // <copyright company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;

using Raven.Tests.Common.Attributes;
using Raven.Tests.Common.Util;

using Xunit;
using Xunit.Extensions;

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
}