// -----------------------------------------------------------------------
//  <copyright file="RavenDB1229.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Tests.Common;

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
				try
				{
					var op = store.DatabaseCommands.DeleteByIndex("noSuchIndex", new IndexQuery
					{
						Query = "Tag:Animals"
					});

					op.WaitForCompletion();

					Assert.False(true, "Should have thrown");
				}
				catch (Exception e)
				{
					Assert.NotNull(e);
				}

			}
		}
	}
}