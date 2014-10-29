// -----------------------------------------------------------------------
//  <copyright file="RavenDB_868.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_868 : RavenTest
	{
		public class CacheEntry{}

		[Fact]
		public void CanQueryUsingAny()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Query<CacheEntry>().Any();
				}
			}
		}
		 
	}
}