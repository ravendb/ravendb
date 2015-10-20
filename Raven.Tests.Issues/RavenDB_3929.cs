// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3929.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Text;

using Jint;
using Jint.Native;
using Jint.Parser;
using Jint.Runtime;

using Raven.Abstractions.Data;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3929 : RavenTest
	{
		[Fact]
		public void NullPropagationShouldNotAffectOperators()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("keys/1", null, new RavenJObject
														   {
															   { "NullField", null },
															   { "NotNullField", "value" },
															   { "EmptyField", "" }
														   }, new RavenJObject());

				store.DatabaseCommands.Patch("keys/1", new ScriptedPatchRequest
				{
					Script = @"
this.is_nullfield_not_null = this.NullField !== null;
this.is_notnullfield_not_null = this.NotNullField !== null;
this.has_emptyfield_not_null = this.EmptyField !== null;
"
				});

				var document = store.DatabaseCommands.Get("keys/1");

				Assert.False(document.DataAsJson.Value<bool>("is_nullfield_not_null"));
				Assert.True(document.DataAsJson.Value<bool>("is_notnullfield_not_null"));
				Assert.True(document.DataAsJson.Value<bool>("has_emptyfield_not_null"));
			}
		}
	}
}