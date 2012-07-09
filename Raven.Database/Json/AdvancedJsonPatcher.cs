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
    //TODO 
    // Need to have a way for the user to specify the current status of the doc
    // then we can throw a ConcurrencyException is the doc is different, see
    // EnsurePreviousValueMatchCurrentValue(..) in JsonPatcher.cs
	public class AdvancedJsonPatcher
	{
		private RavenJObject document;
		private RavenJObject [] documents;
		private bool batchApply = false;
				
		public AdvancedJsonPatcher(RavenJObject document)
		{
			this.document = document;
		}

		public AdvancedJsonPatcher(RavenJObject [] documents)
		{
			this.documents = documents;
			this.batchApply = true;
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

			if (batchApply)
			{
				//foreach (var doc in documents)
				//{
				//    ApplySingleScript(document, script, ctx);
				//}

				//Things to consider, if we get one failed do we stop the whole batch,
				//or do we stop it when we reach a threshold, i.e. 10% failures?
				//Where do failures get logged to, or do they just throw exceptions??
			}
			else
			{
				var resultDocument = ApplySingleScript(ctx, document, script);
				if (resultDocument != null)
					document = resultDocument;
			}
		}

		private RavenJObject ApplySingleScript(CSharp.Context ctx, RavenJObject doc, string script)
		{
			var wrapperScript = String.Format(@"
var doc = {0};

(function(doc){{
	{1}{2}
}}).apply(doc);

json_data = JSON.stringify(doc);", doc, script, script.EndsWith(";") ? String.Empty : ";");

			object result;
			try
			{
				result = ctx.Execute(wrapperScript);
				return RavenJObject.Parse(ctx.GetGlobalAs<string>("json_data"));
			}
			catch (IronJS.UserError uEx)
			{

			}
			catch (IronJS.Error.Error errorEx)
			{

			}			
			return null;
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
