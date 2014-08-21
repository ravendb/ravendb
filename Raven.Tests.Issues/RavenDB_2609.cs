// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2609.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2609 : RavenTest
	{
		[Fact]
		public void ShouldNotOverwriteDocumentIfPatchOpetationDidntModifiedIt()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new Company { Name = "Abc" });
					s.SaveChanges();
				}

				var companyEtag = store.DatabaseCommands.Get("companies/1").Etag;

				store.DatabaseCommands.Patch("companies/1", new ScriptedPatchRequest
				{
					Script = @"this.Name = 'Abc'",
				});

				var afterPatchEtag = store.DatabaseCommands.Get("companies/1").Etag;

				Assert.Equal(companyEtag, afterPatchEtag);
			}
		}
	}
}