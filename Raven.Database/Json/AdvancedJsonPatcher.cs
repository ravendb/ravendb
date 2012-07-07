//-----------------------------------------------------------------------
// <copyright file="AdvancedJsonPatcher.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Json.Linq;
using System.Reflection;
using System.IO;
using IronJS.Hosting;
using Newtonsoft.Json;

namespace Raven.Database.Json
{
	public class AdvancedJsonPatcher
	{
		private RavenJObject document;
				
		public AdvancedJsonPatcher(RavenJObject document)
		{
			this.document = document;
		}

		public RavenJObject Apply(string script)
		{
			ApplyImpl(script);
			return document;
		}

		private void ApplyImpl(string script)
		{            
			var ctx = new CSharp.Context();
			ctx.CreatePrintFunction();
			
			var toJsonScript = GetFromResources("Raven.Database.Json.ToJson.js");
			ctx.Execute(toJsonScript);

			var mapScript = GetFromResources("Raven.Database.Json.Map.js");
			ctx.Execute(mapScript);
			
			var finalScript = String.Format(@"var doc = {0};
(function(doc){{
	{1};
}}).apply(doc);
json_data = JSON.stringify(doc);", document, script);
									
			object result;

			try
			{
				result = ctx.Execute(finalScript);
				document = RavenJObject.Parse(ctx.GetGlobalAs<string>("json_data"));
			}
			catch (IronJS.UserError uEx)
			{

			}
			catch (IronJS.Error.Error errorEx)
			{

			}
			finally
			{

			}
		}

		internal string GetFromResources(string resourceName)
		{
			Assembly assem = this.GetType().Assembly;
			using (Stream stream = assem.GetManifestResourceStream(resourceName))
			{
				using (var reader = new StreamReader(stream))
				{
					return reader.ReadToEnd();
				}
			}
		}
	}
}
