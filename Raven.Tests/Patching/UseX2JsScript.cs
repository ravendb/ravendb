using System;
using System.IO;
using System.Reflection;

using Raven.Abstractions.Data;
using Raven.Database.Json;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Patching
{
	public class UseX2JsScript: IDisposable
	{
		[Fact]
		public void CanProcess()
		{
			var document = new RavenJObject
			{
				{
					"Data", new RavenJObject
					{
						{"Title", "Hi"}
					}
				}
			};

			const string name = @"Raven.Tests.Patching.x2js.js";
			var manifestResourceStream = Assembly.GetExecutingAssembly().GetManifestResourceStream(name);
			var code = new StreamReader(manifestResourceStream).ReadToEnd();

			var jsonPatcher = new ScriptedJsonPatcher();
			using (var scope = new DefaultScriptedJsonPatcherOperationScope())
			{
				scope.CustomFunctions = new RavenJObject
				{
					{"Functions", code}
				};

				jsonPatcher.Apply(scope, document, new ScriptedPatchRequest
				{
					Script = "this.Xml = js2x(this.Data);"
				});
			}
		}

		[Fact]
		public void CanProcess2()
		{
			var document = new RavenJObject
			{
				{
					"Data", new RavenJObject
					{
						{"Title", "Hi"}
					}
				}
			};

			const string name = @"Raven.Tests.Patching.x2js.js";
			var manifestResourceStream = Assembly.GetExecutingAssembly().GetManifestResourceStream(name);
			var code = new StreamReader(manifestResourceStream).ReadToEnd();

			var jsonPatcher = new ScriptedJsonPatcher();
			using (var scope = new DefaultScriptedJsonPatcherOperationScope())
			{
				scope.CustomFunctions =
				new RavenJObject
				{
					{"Functions", code}
				};

				jsonPatcher.Apply(scope, document, new ScriptedPatchRequest
				{
					Script = @"
var item = {
  'Data': { 'Title': 'Hi' }
}

this.Xml = js2x(item.Data);"
				});
			}
		}


		public void Dispose()
		{
			
		}
	}
}
