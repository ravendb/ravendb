// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3264.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Database.Json;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3264
	{
		[Fact]
		public void PatcherCanOutputObjectsCorrectly()
		{
			var doc = RavenJObject.Parse("{}");
			const string script = @"output(undefined);
								output(true);
								output(2);
								output(2.5);
								output('string');
								output(null);
								dump([2, 'c']);
								dump({'a': 'c', 'f': { 'x' : 2}});"
								;

			var patch = new ScriptedPatchRequest()
			{
				Script = script
			};
			using (var scope = new DefaultScriptedJsonPatcherOperationScope())
			{
				var patcher = new ScriptedJsonPatcher();
				patcher.Apply(scope, doc, patch);
				Assert.Equal(8, patcher.Debug.Count);
				Assert.Equal("undefined", patcher.Debug[0]);
				Assert.Equal("True", patcher.Debug[1]);
				Assert.Equal("2", patcher.Debug[2]);
				Assert.Equal("2.5", patcher.Debug[3]);
				Assert.Equal("string", patcher.Debug[4]);
				Assert.Equal("null", patcher.Debug[5]);
				Assert.Equal("[2,\"c\"]", patcher.Debug[6]);
				Assert.Equal("{\"a\":\"c\",\"f\":{\"x\":2}}", patcher.Debug[7]);
			}

			var patchInvalid = new ScriptedPatchRequest()
			{
				Script = "output({ a: 5})"
			};
			var ex = Assert.Throws<Exception>(() =>
			{
				using (var scope = new DefaultScriptedJsonPatcherOperationScope())
				{
					var patcher = new ScriptedJsonPatcher();
					patcher.Apply(scope, doc, patchInvalid);
				}
			});
			Assert.Contains("Use dump()", ex.Message);
		}
	}
}