//-----------------------------------------------------------------------
// <copyright file="ReflectionUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Linq;

namespace Raven.Client.Document
{
	/// <summary>
	/// Helper class for reflection operations
	/// </summary>
	public static class ReflectionUtil
	{
		private static Dictionary<Type, string> fullnameCache = new Dictionary<Type, string>();

		/// <summary>
		/// Gets the full name without version information.
		/// </summary>
		/// <param name="entityType">Type of the entity.</param>
		/// <returns></returns>
		public static string GetFullNameWithoutVersionInformation(Type entityType)
		{
			string result;
			var localFullName = fullnameCache;
			if (localFullName.TryGetValue(entityType, out result))
				return result;

			var asmName = new AssemblyName(entityType.Assembly.FullName).Name;
			if (entityType.IsGenericType)
			{
				var genericTypeDefinition = entityType.GetGenericTypeDefinition();
				var sb = new StringBuilder(genericTypeDefinition.FullName);
				sb.Append("[");
				foreach (var genericArgument in entityType.GetGenericArguments())
				{
					sb.Append("[")
						.Append(GetFullNameWithoutVersionInformation(genericArgument))
						.Append("]");
				}
				sb.Append("], ")
					.Append(asmName);
				result = sb.ToString();
			}
			else
			{
				result = entityType.FullName + ", " + asmName;
			}

			fullnameCache = new Dictionary<Type, string>(localFullName)
			{
				{entityType, result}
			};

			return result;
		}

	}
}
