//-----------------------------------------------------------------------
// <copyright file="ReflectionUtil.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Text;
using System.Linq;

namespace Raven.Client.Document
{
	/// <summary>
	/// Helper class for reflection operations
	/// </summary>
	public static class ReflectionUtil
	{
		/// <summary>
		/// Gets the full name without version information.
		/// </summary>
		/// <param name="entityType">Type of the entity.</param>
		/// <returns></returns>
		public static string GetFullNameWithoutVersionInformation(Type entityType)
		{
			var fullName = entityType.Assembly.FullName ?? "";
			var asmName = fullName.Split(new[] {", Version"}, StringSplitOptions.RemoveEmptyEntries).First();
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
				return sb.ToString();
			}
			return entityType.FullName + ", " + asmName;
		}

	}
}
