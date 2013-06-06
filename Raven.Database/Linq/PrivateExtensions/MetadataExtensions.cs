//-----------------------------------------------------------------------
// <copyright file="MetadataExtensions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Linq;

namespace Raven.Database.Linq.PrivateExtensions
{
	public static class MetadataExtensions
	{
		public static IEnumerable<dynamic> WhereEntityIs(this IEnumerable<dynamic> self, params string[] metadata)
		{
			return self.Where(doc => metadata.Any(
				m => string.Equals(m, doc[Constants.Metadata][Constants.RavenEntityName], StringComparison.InvariantCultureIgnoreCase)));
		}

		public static dynamic IfEntityIs(this object self, string name)
		{
			if (string.Equals(name, ((dynamic)self)[Constants.Metadata][Constants.RavenEntityName], StringComparison.InvariantCultureIgnoreCase))
				return self;

			return new DynamicNullObject();
		}
	}
}