// -----------------------------------------------------------------------
//  <copyright file="WildcardMatching.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Util;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Util
{
	public class WildcardMatching
	{
		[Theory]
		[InlineData("ay?nde", "ayende", true)]
		[InlineData("ay?nde", "ayend", false)]
		[InlineData("rav*b", "RavenDB", true)]
		[InlineData("rav*b", "raven", false)]
		[InlineData("*orders*", "customers/1/orders/123", true)]
		[InlineData("*orders", "customers/1/orders", true)]
		public void CanMatch(string pattern, string input, bool expected)
		{
			Assert.Equal(expected, WildcardMatcher.Matches(pattern, input));
		}
	}
}