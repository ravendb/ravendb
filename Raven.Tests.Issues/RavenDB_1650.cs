// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1650.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
	public class RavenDB_1650 : RavenTest
	{
		public class User
		{
			 
		}

        [Theory]
        [PropertyData("Storages")]
		public void ShouldProperlyDisposeEsentResourcesUsedByStreamingControllerWhenQuerying(string storage)
		{
			using (var store = NewRemoteDocumentStore(requestedStorage: storage))
			{
				using (var session = store.OpenSession())
				{
					var enumerator = session.Advanced.Stream(session.Query<User>(new RavenDocumentsByEntityName().IndexName));
					int count = 0;
					while (enumerator.MoveNext())
					{
						count++;
					}

					Assert.Equal(0, count);
				}
			}
		}
	}
}