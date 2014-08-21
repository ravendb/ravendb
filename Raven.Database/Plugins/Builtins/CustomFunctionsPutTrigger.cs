// -----------------------------------------------------------------------
//  <copyright file="ActiveBundlesProtection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Jint;
using Jint.Parser;

using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Plugins.Builtins
{
    public class CustomFunctionsPutTrigger : AbstractPutTrigger
    {
        public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
        {
            if (key == Constants.RavenJavascriptFunctions)
                return VetoResult.Allowed;

            try
            {
                ValidateCustomFunctions(document);
                return VetoResult.Allowed;
            }
            catch (ParserException e)
            {
                return VetoResult.Deny("Unable to parse custom functions: " + e.Message);
            }
        }

        private void ValidateCustomFunctions(RavenJObject document)
        {
            var engine = new Engine(cfg =>
            {
                cfg.AllowDebuggerStatement();
                cfg.MaxStatements(1000);
            });

            engine.Execute(string.Format(@"
var customFunctions = function() {{ 
	var exports = {{ }};
	{0};
	return exports;
}}();
for(var customFunction in customFunctions) {{
	this[customFunction] = customFunctions[customFunction];
}};", document.Value<string>("Functions")));

        }
    }
}