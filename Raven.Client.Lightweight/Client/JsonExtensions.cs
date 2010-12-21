//-----------------------------------------------------------------------
// <copyright file="JsonExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;

namespace Raven.Client.Client
{
	/// <summary>
	/// Extension to json objects
	/// </summary>
    public static class JsonExtensions
    {
		/// <summary>
		/// Deserializes the specified instance <param name="self"/> to an instance of <typeparam name="T"/> using the specified <param name="convention"/>
		/// </summary>
        public static T Deserialize<T>(this JObject self, DocumentConvention convention)
        {
			return (T)convention.CreateSerializer().Deserialize(new JTokenReader(self), typeof(T));
        }

		/// <summary>
		/// Deserializes the specified instance <param name="self"/> to an instance of <param name="type"/> using the specified <param name="convention"/>
		/// </summary>
		public static object Deserialize(this JObject self, Type type, DocumentConvention convention)
        {
			return convention.CreateSerializer().Deserialize(new JTokenReader(self), type);
        }
    }
}
