// -----------------------------------------------------------------------
//  <copyright file="StrangeNames.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class StrangeNames : RavenTest
	{
		 public class \u211B
		 {
		 } 

		 [Fact]
		 public void CanLoadAndGet()
		 {
			 using (var store = NewRemoteDocumentStore())
			 {
				 using (var s = store.OpenSession())
				 {
					s.Store(new \u211B(), "id");
					 s.SaveChanges();
				 }

				 using (var s = store.OpenSession())
				 {
					 var load = s.Advanced.DocumentStore.DatabaseCommands.Get("id");
					 Assert.NotNull(load);

					 Assert.Contains(typeof(\u211B).FullName, load.Metadata.Value<string>(Constants.RavenClrType));

					 Assert.Contains(typeof(\u211B).Name + "s", load.Metadata.Value<string>(Constants.RavenEntityName));

				 }
			 }
		 }
	}
}