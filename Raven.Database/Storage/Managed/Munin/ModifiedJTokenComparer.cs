//-----------------------------------------------------------------------
// <copyright file="ModifiedJTokenComparer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Munin
{
	public class ModifiedJTokenComparer : RavenJTokenComparer
	{
		private readonly Func<RavenJToken, RavenJToken> modifier;

		public ModifiedJTokenComparer(Func<RavenJToken, RavenJToken> modifier)
		{
			this.modifier = modifier;
		}

		public override int Compare(RavenJToken x, RavenJToken y)
		{
			var localX = x.Type == JTokenType.Object ? modifier(x) : x;
			var localY = y.Type == JTokenType.Object ? modifier(y) : y;
			return base.Compare(localX, localY);
		}

		public override int GetHashCode(RavenJToken obj)
		{
			var localObj = obj.Type == JTokenType.Object ? modifier(obj) : obj;
			return base.GetHashCode(localObj);
		}
	}
}