// -----------------------------------------------------------------------
//  <copyright file="RavenDB_425.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Tests.Bugs;
using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB_425 : RavenTest
	{
		[Fact]
	 	public void WillGetErrorWhenQueryingById()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					var invalidOperationException = Assert.Throws<InvalidOperationException>(() => 
						session.Query<CannotChangeId.Item>().Where(x => x.Id == "items/1").ToList());

					Assert.Contains("Attempt to query by id only is blocked, you should use call session.Load(\"items/1\"); instead of session.Query().Where(x=>x.Id == \"items/1\");", invalidOperationException.Message);
				}
			}
		}
	}
}