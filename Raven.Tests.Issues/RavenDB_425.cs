// -----------------------------------------------------------------------
//  <copyright file="RavenDB_425.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

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
						session.Query<User>().Where(x => x.Id == "users/1").ToList());

					Assert.Contains("Attempt to query by id only is blocked, you should use call session.Load(\"users/1\"); instead of session.Query().Where(x=>x.Id == \"users/1\");", invalidOperationException.Message);
				}
			}
		}
	}
}