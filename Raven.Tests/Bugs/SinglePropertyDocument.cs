//-----------------------------------------------------------------------
// <copyright file="SinglePropertyDocument.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SinglePropertyDocument : RavenTest
	{
		[Fact]
		public void CanSaveDocumentWithJustId()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Email());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.NotNull(session.Load<Email>("emails/1"));
				}
			}
		}

		public class Email
		{
			public string Id { get; set; }
		}
	}
}
