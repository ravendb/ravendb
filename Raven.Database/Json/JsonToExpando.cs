//-----------------------------------------------------------------------
// <copyright file="JsonToExpando.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Newtonsoft.Json.Linq;
using Raven.Database.Linq;

namespace Raven.Database.Json
{
	public static class JsonToExpando
	{
		public static object Convert(JObject obj)
		{
			return new DynamicJsonObject(obj);
		}
	}
}
