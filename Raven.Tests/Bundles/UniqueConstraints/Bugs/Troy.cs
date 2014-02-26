// //-----------------------------------------------------------------------
// // <copyright file="Troy.cs" company="Hibernating Rhinos LTD">
// //     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// // </copyright>
// //-----------------------------------------------------------------------
using System;

using Raven.Client.UniqueConstraints;

using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
	public class Troy : UniqueConstraintsTest
	{
		public class UserAccount
		{
			[UniqueConstraint(CaseInsensitive = true)]
			public string UserName { get; set; }
			public string Email { get; set; }
			public string FirstName { get; set; }
			public string LastName { get; set; }
			public string Password { get; set; }
			public bool Deleted { get; set; }
		}

		[Fact]
		public void CheckForUniqueConstraints_Letter_Casing_Of_Contrained_Fields_Behaves_Differently_Than_SaveChanges()
		{
			DocumentStore.Conventions.IdentityPartsSeparator = "-";
			DocumentStore.Conventions.TransformTypeTagNameToDocumentKeyPrefix = PreserveTypeTagNameToDocumentKeyPrefix;

			var username = "Test" + DateTime.Now.Ticks;
			var email = username.ToLower() + "@test.com";
			var model1 = new UserAccount {UserName = username, Email = email, FirstName = "Troy", LastName = "Testing", Password = "testing", Deleted = false};
			var model2 = new UserAccount { UserName = username.ToUpper(), Email = email, FirstName = "Troy", LastName = "Testing", Password = "testing", Deleted = false };
			var model3 = new UserAccount { UserName = username.ToUpper(), Email = email.ToUpper(), FirstName = "Troy", LastName = "Testing", Password = "testing", Deleted = false };
			using (var session = DocumentStore.OpenSession())
			{
				session.Store(model1);
				session.SaveChanges();
			}
			using (var session = DocumentStore.OpenSession())
			{
				var results = session.CheckForUniqueConstraints(model2);
				Assert.False(results.ConstraintsAreFree());
			}
		}
		
		private static string PreserveTypeTagNameToDocumentKeyPrefix(string typeTagName)
		{
			return typeTagName;
		} 

	}
}