//-----------------------------------------------------------------------
// <copyright file="PatchRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Raven.Database.Json
{
    /// <summary>
	/// A patch request for a specified document
	/// </summary>
	public class PatchRequest
	{
		/// <summary>
		/// Gets or sets the type of the operation
		/// </summary>
		/// <value>The type.</value>
		public PatchCommandType Type { get; set; }
		/// <summary>
		/// Gets or sets the previous val, which is compared against the current value to verify a
		/// change isn't overwriting new values.
		/// If the value is null, the operation is always successful
		/// </summary>
		/// <value>The previous val.</value>
		public JToken PrevVal { get; set; }
		/// <summary>
		/// Gets or sets the value.
		/// </summary>
		/// <value>The value.</value>
		public JToken Value { get; set; }
		/// <summary>
		/// Gets or sets the nested operations to perform. This is only valid when the <see cref="Type"/> is <see cref="PatchCommandType.Modify"/>.
		/// </summary>
		/// <value>The nested.</value>
		public PatchRequest[] Nested { get; set; }
		/// <summary>
		/// Gets or sets the name.
		/// </summary>
		/// <value>The name.</value>
		public string Name { get; set; }
		/// <summary>
		/// Gets or sets the position.
		/// </summary>
		/// <value>The position.</value>
		public int? Position { get; set; }

		/// <summary>
		/// Translate this instance to json
		/// </summary>
		public JObject ToJson()
		{
			var jObject = new JObject(
				new JProperty("Type", new JValue(Type.ToString())),
				new JProperty("Value", Value),
				new JProperty("Name", new JValue(Name)),
				new JProperty("Position", Position == null ? null : new JValue(Position.Value)),
				new JProperty("Nested", Nested == null ? null : new JArray(Nested.Select(x => x.ToJson())))
				);
			if (PrevVal != null)
				jObject.Add(new JProperty("PrevVal", PrevVal));
			return jObject;
		}

		/// <summary>
		/// Create an instance from a json object
		/// </summary>
		/// <param name="patchRequestJson">The patch request json.</param>
		public static PatchRequest FromJson(JObject patchRequestJson)
		{
			PatchRequest[] nested = null;
			var nestedJson = patchRequestJson.Value<JToken>("Nested");
            if (nestedJson != null && nestedJson.Type != JTokenType.Null)
                nested = patchRequestJson.Value<JArray>("Nested").Cast<JObject>().Select(FromJson).ToArray();

			return new PatchRequest
			{
				Type = (PatchCommandType)Enum.Parse(typeof(PatchCommandType), patchRequestJson.Value<string>("Type"), true),
				Name = patchRequestJson.Value<string>("Name"),
				Nested = nested,
				Position = patchRequestJson.Value<int?>("Position"),
				PrevVal = patchRequestJson.Property("PrevVal") == null ? null : patchRequestJson.Property("PrevVal").Value,
				Value = patchRequestJson.Property("Value") == null ? null : patchRequestJson.Property("Value").Value,
			};
		}
	}
}
