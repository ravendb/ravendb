// -----------------------------------------------------------------------
//  <copyright file="Wayne.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Wayne : RavenTest
	{
		public class User
		{
			public int Friends;
		}
		[Fact]
		public void CanGetOutputFromScript_UpdateByIndex()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User());
					s.SaveChanges();
				}

				WaitForIndexing(store);

				var op = store.DatabaseCommands.UpdateByIndex("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:Users"
				}, new ScriptedPatchRequest
				{
					Script = @"
					this.Friends += 1;
					output(this.Friends);
					"
				});

				var state = (RavenJArray)op.WaitForCompletion();
				Assert.Contains("1", state[0].Value<RavenJArray>("Debug")[0].ToString(Formatting.None));

			}
		}

		[Fact]
		public void CanGetOutputFromScript_UpdateByIndex_Remote()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User());
					s.SaveChanges();
				}

				WaitForIndexing(store);

				var op = store.DatabaseCommands.UpdateByIndex("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:Users"
				}, new ScriptedPatchRequest
				{
					Script = @"
					this.Friends += 1;
					output(this.Friends);
					"
				});

				var state = (RavenJArray)op.WaitForCompletion();
				Assert.Contains("1", state[0].Value<RavenJArray>("Debug")[0].ToString(Formatting.None));

			}
		}

		[Fact]
		public void CanGetOutputFromScript_SingleOp()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User());
					s.SaveChanges();
				}

				var a= store.DatabaseCommands.Patch("users/1", new ScriptedPatchRequest
				{
					Script = @"
					this.Friends += 1;
					output(this.Friends);
					"
				});

				Assert.Contains("1", a.Value<RavenJArray>("Debug")[0].ToString(Formatting.None));
			}
		}

		[Fact]
		public void CanGetOutputFromScript_SingleOp_Remote()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new User());
					s.SaveChanges();
				}

				var a = store.DatabaseCommands.Patch("users/1", new ScriptedPatchRequest
				{
					Script = @"
					this.Friends += 1;
					output(this.Friends);
					"
				});

				Assert.Contains("1", a.Value<RavenJArray>("Debug")[0].ToString(Formatting.None));
			}
		}
	}
}