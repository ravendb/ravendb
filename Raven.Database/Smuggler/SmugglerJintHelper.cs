// -----------------------------------------------------------------------
//  <copyright file="SmugglerJintHelper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

using Jint;
using Jint.Native;

using Raven.Abstractions.Smuggler;
using Raven.Database.Json;
using Raven.Json.Linq;

namespace Raven.Database.Smuggler
{
	public class SmugglerJintHelper
	{
		private Engine jint;

		public void Initialize(SmugglerDatabaseOptions databaseOptions)
		{
			if (databaseOptions == null || string.IsNullOrEmpty(databaseOptions.TransformScript))
				return;

			jint = new Engine(cfg =>
			{
				cfg.AllowDebuggerStatement(false);
				cfg.MaxStatements(databaseOptions.MaxStepsForTransformScript);
				cfg.NullPropagation();
			});

			jint.Execute(string.Format(@"
					function Transform(docInner){{
						return ({0}).apply(this, [docInner]);
					}};", databaseOptions.TransformScript));
		}

		public RavenJObject Transform(string transformScript, RavenJObject input)
		{
			if (jint == null)
				throw new InvalidOperationException("Jint must be initialized.");

			jint.ResetStatementsCount();

			using (var scope = new OperationScope())
			{
				var jsObject = scope.ToJsObject(jint, input);
				var jsObjectTransformed = jint.Invoke("Transform", jsObject);

				return jsObjectTransformed != JsValue.Null ? scope.ConvertReturnValue(jsObjectTransformed) : null;
			}
		}

		private class OperationScope : JintOperationScope
		{
		}
	}
}
