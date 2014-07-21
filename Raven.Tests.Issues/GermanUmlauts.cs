// -----------------------------------------------------------------------
//  <copyright file="GermanUmlauts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class GermanUmlauts : RavenTest
	{
		public class Gespräch
		{
			public string BeurteilungId { get; set; }
		}

		[Fact]
		public void CanQueryUsingAutoIndex()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Query<Gespräch>()
						.Where(x => x.BeurteilungId != null)
						.ToList();
				}
			}
		}

		[Fact]
		public void CanQueryUsingAutoIndex_Remote()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Query<Gespräch>()
						.Where(x => x.BeurteilungId != null)
						.ToList();
				}
			}
		}
	}
}