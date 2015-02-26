// -----------------------------------------------------------------------
//  <copyright file="JavascriptFunctionsRetriever.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Config.Retriever
{
	public class JavascriptFunctionsRetriever : ConfigurationRetrieverBase<RavenJObject>
	{
		protected override RavenJObject ApplyGlobalDocumentToLocal(RavenJObject global, RavenJObject local, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			local["Functions"] = global.Value<string>("Functions") + ";" + local.Value<string>("Functions");

			return local;
		}

		protected override RavenJObject ConvertGlobalDocumentToLocal(RavenJObject global, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			global.Value<RavenJObject>("@metadata")["@id"] = Constants.RavenJavascriptFunctions;

			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			return Constants.Global.JavascriptFunctions;
		}
	}
}