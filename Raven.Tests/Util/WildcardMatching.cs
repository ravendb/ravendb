// -----------------------------------------------------------------------
//  <copyright file="WildcardMatching.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Util;
using Raven.Tests.Stress;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Util
{
	public class WildcardMatching
	{
		[Theory]
		[InlineValue("ay?nde", "ayende", true)]
		[InlineValue("ay?nde", "ayend", false)]
		[InlineValue("rav*b", "RavenDB", true)]
		[InlineValue("rav*b", "raven", false)]
		[InlineValue("*orders*", "customers/1/orders/123", true)]
		[InlineValue("*orders", "customers/1/orders", true)]
		public void CanMatch(string pattern, string input, bool expected)
		{
			Assert.Equal(expected, WildcardMatcher.Matches(pattern, input));
		}
	}
}