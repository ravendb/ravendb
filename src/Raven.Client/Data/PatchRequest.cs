//-----------------------------------------------------------------------
// <copyright file="PatchRequest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Client.Data
{
    /// <summary>
    /// A patch request for a specified document
    /// </summary>
    public class PatchRequest
    {
        /// <summary>
        /// Type of patch operation.
        /// </summary>
        public PatchCommandType Type { get; set; }

        /// <summary>
        /// Gets or sets the previous value, which is compared against the current value to verify a
        /// change isn't overwriting new values.
        /// <para>If the value is <c>null</c>, the operation is always successful</para>
        /// </summary>
        public RavenJToken PrevVal { get; set; }

        /// <summary>
        /// New value.
        /// </summary>
        public RavenJToken Value { get; set; }

        /// <summary>
        /// Nested operations to perform. This is only valid when the <see cref="Type"/> is <see cref="PatchCommandType.Modify"/>.
        /// </summary>
        public PatchRequest[] Nested { get; set; }

        /// <summary>
        /// Property/field name to patch.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Only valid for collection types. Position in collection to patch.
        /// </summary>
        public int? Position { get; set; }

        /// <summary>
        /// Only valid for collections. Set this property to true if you want to modify all items in an collection.
        /// </summary>
        public bool? AllPositions { get; set; }

        /// <summary>
        /// Translate this instance to json
        /// </summary>
        public RavenJObject ToJson()
        {
            var jObject = new RavenJObject
                            {
                                {"Type", new RavenJValue(Type.ToString())},
                                {"Value", Value},
                                {"Name", new RavenJValue(Name)}
                            };
            if (Position != null)
                jObject.Add("Position", new RavenJValue(Position.Value));
            if (Nested != null)
                jObject.Add("Nested",  new RavenJArray(Nested.Select(x => x.ToJson())));
            if (AllPositions != null)
                jObject.Add("AllPositions", new RavenJValue(AllPositions.Value));
            if (PrevVal != null)
                jObject.Add("PrevVal", PrevVal);
            return jObject;
        }

        /// <summary>
        /// Create an instance from a json object
        /// </summary>
        public static PatchRequest FromJson(RavenJObject patchRequestJson)
        {
            PatchRequest[] nested = null;
            var nestedJson = patchRequestJson.Value<RavenJToken>("Nested");
            if (nestedJson != null && nestedJson.Type != JTokenType.Null)
                nested = patchRequestJson.Value<RavenJArray>("Nested").Cast<RavenJObject>().Select(FromJson).ToArray();

            return new PatchRequest
            {
                Type = (PatchCommandType)Enum.Parse(typeof(PatchCommandType), patchRequestJson.Value<string>("Type"), true),
                Name = patchRequestJson.Value<string>("Name"),
                Nested = nested,
                Position = patchRequestJson.Value<int?>("Position"),
                AllPositions = patchRequestJson.Value<bool?>("AllPositions"),
                PrevVal = patchRequestJson["PrevVal"],
                Value = patchRequestJson["Value"],
            };
        }
    }
}
