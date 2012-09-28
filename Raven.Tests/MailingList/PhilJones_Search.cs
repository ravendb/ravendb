// -----------------------------------------------------------------------
//  <copyright file="PhilJones.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client;
using Xunit;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class PhilJones_Search : RavenTest
	{
		[Fact]
		public void CanChangeParsingOfSearchQueries()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					Assert.Equal(@"FirstName:(\*Ore\?n\*)", session.Query<User>()
						.Search(x => x.FirstName, "*Ore?n*", escapeQueryOptions: EscapeQueryOptions.EscapeAll)
						.ToString());

					Assert.Equal(@"FirstName:(*Ore?n*)", session.Query<User>()
						.Search(x => x.FirstName, "*Ore?n*", escapeQueryOptions: EscapeQueryOptions.RawQuery)
						.ToString());


					Assert.Equal(@"FirstName:(*Ore\?n*)", session.Query<User>()
						.Search(x => x.FirstName, "*Ore?n*", escapeQueryOptions: EscapeQueryOptions.AllowAllWildcards)
						.ToString());

					Assert.Equal(@"FirstName:(\*Ore\?n*)", session.Query<User>()
						.Search(x => x.FirstName, "*Ore?n*", escapeQueryOptions: EscapeQueryOptions.AllowPostfixWildcard)
						.ToString());
				}
			}
		}
		 
	}
}