// -----------------------------------------------------------------------
//  <copyright file="AggressiveCachingEmbedded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class AggressiveCachingEmbedded : RavenTest
	{
		[Fact]
		public void CanUseIt()
		{
			using (var store = NewDocumentStore())
			{
				using (store.AggressivelyCache())
				{
					using (var session = store.OpenSession())
					{
						
					}
				}
			}
		}
	}
}