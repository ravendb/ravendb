// -----------------------------------------------------------------------
//  <copyright file="Garrett.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;

using Raven.Tests.Common;

using Xunit;
using System.Linq;
using Raven.Client.Linq;

namespace Raven.Tests.MailingList
{
	public class Garrett : RavenTest
	{
		public class StrategyIndividual
		{
			public string OtherProp { get; set; }
			public Dictionary<int, double> Statistcs { get; set; }
		}

		[Fact]
		public void CanOrderByDictionaryValue()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Query<StrategyIndividual>()
				     .Where(x=>x.Statistcs[4] == 0)
					 .OrderBy(x => x.Statistcs[4])
					 .ToList();
				}
			}
		}
	}
}