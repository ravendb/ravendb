//-----------------------------------------------------------------------
// <copyright file="GroupByKeyFunc.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Database.Linq
{
	/// <summary>
	/// Get the group by value from the document
	/// </summary>
	public delegate dynamic GroupByKeyFunc(dynamic source);
}