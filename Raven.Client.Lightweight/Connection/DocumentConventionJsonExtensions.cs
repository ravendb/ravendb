//-----------------------------------------------------------------------
// <copyright file="DocumentConventionJsonExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Raven.Client.Connection
{
	/// <summary>
	/// Extension to json objects
	/// </summary>
	public static class DocumentConventionJsonExtensions
	{
		/// <summary>
		/// Deserializes the specified instance <param name="self"/> to an instance of <typeparam name="T"/> using the specified <param name="convention"/>
		/// </summary>
		public static T Deserialize<T>(this RavenJObject self, DocumentConvention convention)
		{
			return (T)convention.CreateSerializer().Deserialize(new RavenJTokenReader(self), typeof(T));
		}

		/// <summary>
		/// Deserializes the specified instance <param name="self"/> to an instance of <param name="type"/> using the specified <param name="convention"/>
		/// </summary>
		public static object Deserialize(this RavenJObject self, Type type, DocumentConvention convention)
		{
			return convention.CreateSerializer().Deserialize(new RavenJTokenReader(self), type);
		}
	}
}
