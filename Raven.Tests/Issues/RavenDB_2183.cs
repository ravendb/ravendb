// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2183.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Abstractions.Linq;

	using Xunit;

	public class RavenDB_2183 : RavenTest
	{
		private class Address
		{
			public string Street { get; set; }
		}

		[Fact]
		public void DynamicListShouldContainTakeMethod()
		{
			var list =
				new DynamicList(
					new List<Address>
					{
						new Address { Street = "Street1" },
						new Address { Street = "Street2" },
						new Address { Street = "Street3" }
					});

			Assert.Equal(2, list.Take(2).Count());
		}
	}
}