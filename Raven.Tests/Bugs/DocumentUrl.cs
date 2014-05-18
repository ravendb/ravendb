//-----------------------------------------------------------------------
// <copyright file="DocumentUrl.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Raven.Database.Server;
using Raven.Tests.Common;
using Raven.Tests.Indexes;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Bugs
{
	public class DocumentUrl : RavenTest
	{
		[Fact]
		public void CanGetFullUrl_WithSlashOnTheEnd()
		{
			using (var documentStore = NewRemoteDocumentStore(fiddler: true))
			{
				using (var session = documentStore.OpenSession())
				{

					var entity = new LinqIndexesFromClient.User();
					session.Store(entity);

					var storedUrl = session.Advanced.GetDocumentUrl(entity);

					//replace machine name with localhost
					var correctedStoredUrl = storedUrl.Replace(Environment.MachineName.ToLower(),"localhost");

                    Assert.Equal("http://localhost:8079/databases/CanGetFullUrl_WithSlashOnTheEnd/docs/users/1", correctedStoredUrl);
				}
			}
		}
	}
}