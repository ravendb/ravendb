// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2496.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Bundles.MoreLikeThis;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2496 : RavenTest
	{
		[Fact]
		public void TransformersShouldWorkWithMoreLikeThis1()
		{
			using (var x = new MoreLikeThisTests())
			{
				x.TransformersShouldWorkWithMoreLikeThis1();
			}
		}

		[Fact]
		public void TransformersShouldWorkWithMoreLikeThis2()
		{
			using (var x = new MoreLikeThisTests())
			{
				x.TransformersShouldWorkWithMoreLikeThis2();
			}
		}
	}
}