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
	public class ScriptedPatchRequest
	{
		/// <summary>
		/// The JavaScript function to use the patch a document
		/// </summary>
		/// <value>The type.</value>
		public string Script { get; set; }

		public Dictionary<string, object> Values { get; set; }

		public ScriptedPatchRequest()
		{
			Values = new Dictionary<string, object>();
		}

		public static ScriptedPatchRequest FromJson(RavenJObject patchRequestJson)
		{
			return patchRequestJson.JsonDeserialization<ScriptedPatchRequest>();
		}

		protected bool Equals(ScriptedPatchRequest other)
		{
			if(other == null)
				return false;
			return string.Equals(Script, other.Script) && Values.Keys.SequenceEqual(other.Values.Keys);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			return Equals(obj as ScriptedPatchRequest);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				return Values.Keys.Aggregate(Script.GetHashCode()*397, (i, s) => i*397 ^ s.GetHashCode());
			}
		}
	}
}
