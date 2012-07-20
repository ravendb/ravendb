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
		public String Script { get; set; }

		/// <summary>
		/// Gets or sets the previous value, which is compared against the current value to verify a
		/// change isn't overwriting new values.
		/// If the value is null, the operation is always successful
		/// </summary>
		/// <value>The previous value.</value>
		public RavenJObject PrevVal { get; set; }

		public static AdvancedPatchRequest FromJson(RavenJObject patchRequestJson)
		{
			return patchRequestJson.JsonDeserialization<AdvancedPatchRequest>();
		}
	}
}
