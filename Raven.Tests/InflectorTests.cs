// -----------------------------------------------------------------------
//  <copyright file="InflectorTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Util;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests
{
	public class InflectorTests
	{
		[Theory]
		[InlineData("User", "Users")]
		[InlineData("Users", "Users")]
		[InlineData("tanimport", "tanimports")]
		[InlineData("tanimports", "tanimports")]
		public void CanUsePluralizeSafelyOnMaybeAlreadyPluralizedWords(string word, string expected)
		{
			Assert.Equal(expected, Inflector.Pluralize(word));
		}
	}
}