//-----------------------------------------------------------------------
// <copyright file="IndexingFunc.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Database.Linq
{
	/// <summary>
	/// 	Defining the indexing function for a set of documents
	/// </summary>
	public delegate IEnumerable<dynamic> IndexingFunc(IEnumerable<dynamic> source);
}
