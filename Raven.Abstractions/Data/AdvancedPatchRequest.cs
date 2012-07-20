using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Abstractions.Data
{
	/// <summary>
	/// A Advanced patch request for a specified document (using JavaScript)
	/// </summary>
	public class AdvancedPatchRequest
	{
		/// <summary>
		/// The JavaScript function to use the patch a document
		/// </summary>
		/// <value>The type.</value>
		public string Script { get; set; }

		public static AdvancedPatchRequest FromJson(RavenJObject patchRequestJson)
		{
			return patchRequestJson.JsonDeserialization<AdvancedPatchRequest>();
		}
	}
}
