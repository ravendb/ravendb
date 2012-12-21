//-----------------------------------------------------------------------
// <copyright file="ExpressionInfo.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Client.Linq
{
	/// <summary>
	/// This class represents a node in an expression, usually a member - but in the case of dynamic queries the path to a member
	/// </summary>
	public class ExpressionInfo
	{
		/// <summary>
		/// Gets the full path of the member being referred to by this node
		/// </summary>
		public string Path { get; private set; }

		/// <summary>
		/// Gets the actual type being referred to
		/// </summary>
		public Type Type { get; private set; }

		/// <summary>
		/// Whatever the expression is of a nested path
		/// </summary>
		public bool IsNestedPath { get; private set; }

		/// <summary>
		/// Creates an ExpressionMemberInfo
		/// </summary>
		public ExpressionInfo(string path, Type type, bool isNestedPath)
		{
			IsNestedPath = isNestedPath;
			Path = path;
			Type = type;
		}
	}
}
