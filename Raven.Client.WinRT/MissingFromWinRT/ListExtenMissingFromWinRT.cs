// -----------------------------------------------------------------------
//  <copyright file="ListExtenMissingFromWinRT.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Raven.Client.WinRT.MissingFromWinRT
{
	public static class ListExtenMissingFromWinRT
	{
		/// <summary>
		/// Returns a read-only <see cref="T:System.Collections.Generic.IList`1"/> wrapper for the current collection.
		/// </summary>
		/// 
		/// <returns>
		/// A <see cref="T:System.Collections.ObjectModel.ReadOnlyCollection`1"/> that acts as a read-only wrapper around the current <see cref="T:System.Collections.Generic.List`1"/>.
		/// </returns>
		public static ReadOnlyCollection<T> AsReadOnly<T>(this IList<T> list)
		{
			return new ReadOnlyCollection<T>(list);
		} 
	}
}