// //-----------------------------------------------------------------------
// // <copyright file="JimBolla.cs" company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.UniqueConstraints;
using Xunit.Extensions;

namespace Raven.Bundles.Tests.UniqueConstraints.Bugs
{
	public class JimBolla : UniqueConstraintsTest
	{
		public class App
		{
			public string Id { get; set; }

			[UniqueConstraint]
			public string Name { get; set; }

			public string Realm { get; set; }

			[UniqueConstraint]
			public string RealmConstraint
			{
				get { return Realm == null ? null : Realm + "x_x"; }
				set { }
			}
		}

		[Theory]
		[InlineData("Name", "http://localhost/")] // green
		[InlineData(null, "http://localhost/")] // exception on SaveChanges()
		[InlineData("Name", null)] // exception on SaveChanges()
		public void Test(string name, string realm)
		{
			using (var raven = DocumentStore.OpenSession())
			{
				raven.Store(new App { Name = name, Realm = realm });
				raven.SaveChanges();
			}
		}
	}
}