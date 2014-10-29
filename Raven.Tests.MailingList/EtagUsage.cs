// -----------------------------------------------------------------------
//  <copyright file="EtagUsage.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Exceptions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class EtagUsage : RavenTest
	{
		[Fact]
		public void TryingToOverwriteItemWithNewOneWouldFail()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("users/1", null, new RavenJObject(), new RavenJObject());

				using (var session = store.OpenSession())
				{
					session.Store(new User());
					Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
				}
			}
		}
	}
}