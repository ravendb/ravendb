//-----------------------------------------------------------------------
// <copyright file="GuidExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;

namespace Raven.Database.Extensions
{
	public static class GuidExtensions
	{
		public static Guid TransfromToGuidWithProperSorting(this byte[] bytes)
		{
			if (bytes == null)
				return Guid.Empty;

			return new Guid(bytes);
		}

		public static byte[] TransformToValueForEsentSorting(this Guid guid)
		{
			return guid.ToByteArray();
		}
	}
}
