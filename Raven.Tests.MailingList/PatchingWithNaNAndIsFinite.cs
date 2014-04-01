// -----------------------------------------------------------------------
//  <copyright file="PatchingWithNaNAndIsFinite.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class PatchingWithNaNAndIsFinite : NoDisposalNeeded
	{
		[Fact]
		public void ShouldWork()
		{
			var scriptedJsonPatcher = new ScriptedJsonPatcher();
			var result = scriptedJsonPatcher.Apply(new RavenJObject {{"Val", double.NaN}}, new ScriptedPatchRequest
			{
				Script = @"this.Finite = isFinite(this.Val);"
			});

			Assert.False(result.Value<bool>("Finite"));
		}
	}
}