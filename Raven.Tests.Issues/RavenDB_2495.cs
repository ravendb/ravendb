// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2495.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Bundles.MoreLikeThis;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2495 : RavenTest
	{
		[Fact]
		public void IncludesShouldWorkWithMoreLikeThis()
		{
			using (var x = new MoreLikeThisTests())
			{
				x.IncludesShouldWorkWithMoreLikeThis();
			}
		}
	}
}