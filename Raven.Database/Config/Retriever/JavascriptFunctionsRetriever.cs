// -----------------------------------------------------------------------
//  <copyright file="JavascriptFunctionsRetriever.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;

namespace Raven.Database.Config.Retriever
{
	public class JavascriptFunctionsRetriever : ConfigurationRetrieverBase<JsonDocument>
	{
		protected override JsonDocument ApplyGlobalDocumentToLocal(JsonDocument global, JsonDocument local, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			local.DataAsJson["Functions"] = global.DataAsJson["Functions"] + " " + local.DataAsJson["Functions"];

			return local;
		}

		protected override JsonDocument ConvertGlobalDocumentToLocal(JsonDocument global, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			global.Key = Constants.RavenJavascriptFunctions;
			global.Metadata["@id"] = Constants.RavenJavascriptFunctions;

			return global;
		}

		public override string GetGlobalConfigurationDocumentKey(string key, DocumentDatabase systemDatabase, DocumentDatabase localDatabase)
		{
			return Constants.Global.JavascriptFunctions;
		}
	}
}