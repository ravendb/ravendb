// -----------------------------------------------------------------------
//  <copyright file="UrlRepro.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class UrlInIdsRepro : RavenTest
	{
		public class Login
		{
			public const string DocumentTypePrefix = "logins/";

			public string Id { get; private set; }

			private Login() { }

			public static Login Create(string claimedId)
			{
				// Both of these variants fail, make sure to clear the database before running this test though!
				//string encodedIdentifier = System.Web.HttpUtility.UrlEncode( claimedId );
				string encodedIdentifier = claimedId;

				return new Login()
				{
					Id = Login.DocumentTypePrefix + encodedIdentifier
				};
			}
		}

		[Fact]
		//[TimeBombedFact(2013, 12, 31)]
		public void CanSaveAndRetrieveTestOpenId()
		{
			using (var store = NewRemoteDocumentStore())
			{
				string id;
				using (var session = store.OpenSession())
				{
					var login = Login.Create("https://me.yahoo.com/a/rlvVJyIHwuykvNWYWOrE_Uv3Jt_d#c2458");
					session.Store(login);
					session.SaveChanges();

					id = login.Id;
				}

				using (var session = store.OpenSession())
				{
					var doc = session.Load<Login>(id);
					Assert.NotNull(doc);
				}
			}
		}
	}
}