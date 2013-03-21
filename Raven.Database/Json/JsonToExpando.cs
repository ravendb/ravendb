//-----------------------------------------------------------------------
// <copyright file="JsonToExpando.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Linq;
using Raven.Database.Linq;
using Raven.Json.Linq;

namespace Raven.Database.Json
{
	public static class JsonToExpando
	{
		public static object Convert(RavenJObject obj)
		{
			return new DynamicJsonObject(obj);
		}
	}
}
