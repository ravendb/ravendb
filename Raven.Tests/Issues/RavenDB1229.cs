// -----------------------------------------------------------------------
//  <copyright file="RavenDB1229.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB1229 : RavenTest
	{
		[Fact]
		public void DeleteByNotExistingIndex()
		{
			using (var store = NewRemoteDocumentStore())
			{

				var op = store.DatabaseCommands.DeleteByIndex("noSuchIndex", new IndexQuery
				{
					Query = "Tag:Animals"
				});

				Assert.Throws<InvalidOperationException>(() => op.WaitForCompletion());

			}
		}
	}
}