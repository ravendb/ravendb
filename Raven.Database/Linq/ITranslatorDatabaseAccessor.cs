//-----------------------------------------------------------------------
// <copyright file="ITranslatorDatabaseAccessor.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;

namespace Raven.Database.Linq
{
	/// <summary>
	/// This is used to provide a way for a translator function 
	/// to access values from the database
	/// </summary>
	public interface ITranslatorDatabaseAccessor
	{
		/// <summary>
		/// Returns the document matching this id, if exists, or null if it doesn't
		/// </summary>
		dynamic Load(object maybeId);

		/// <summary>
		/// Returns null object
		/// </summary>
		dynamic Include(object maybeId);
	}
}
