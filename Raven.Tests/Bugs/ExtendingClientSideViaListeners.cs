//-----------------------------------------------------------------------
// <copyright file="ExtendingClientSideViaListeners.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class ExtendingClientSideViaListeners : RavenTest
	{
		[Fact]
		public void CanFailSave()
		{
			using (var store = NewDocumentStore()
				.RegisterListener(new FailStore()))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Email());
					Assert.Throws<NotImplementedException>(() => session.SaveChanges());
				}
			}
		}

		[Fact]
		public void CanFailDelete()
		{
			using (var store = NewDocumentStore()
				.RegisterListener(new FailDelete()))
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Email());
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Delete(session.Load<Email>("emails/1"));
					Assert.Throws<NotImplementedException>(() => session.SaveChanges());
				}
			}
		}

		public class Email
		{
			public string Id { get; set; }
		}
	}
}
