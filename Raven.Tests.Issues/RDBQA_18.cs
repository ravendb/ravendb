// -----------------------------------------------------------------------
//  <copyright file="RDBQA_18.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RDBQA_18 : RavenTest
	{
		[Fact]
		public void ShouldNotThrowNullReferenceException()
		{
			using (var store = new DocumentStore())
			{
				Assert.DoesNotThrow(store.Replication.WaitAsync().Wait);
			}
		}
	}
}