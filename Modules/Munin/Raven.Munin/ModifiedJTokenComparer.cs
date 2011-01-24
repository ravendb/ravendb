//-----------------------------------------------------------------------
// <copyright file="ModifiedJTokenComparer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;

namespace Raven.Munin
{
    public class ModifiedJTokenComparer : JTokenComparer
    {
        private readonly Func<JToken, JToken> modifier;

        public ModifiedJTokenComparer(Func<JToken, JToken> modifier)
        {
            this.modifier = modifier;
        }

        public override int Compare(JToken x, JToken y)
        {
            var localX = x.Type == JTokenType.Object ? modifier(x) : x;
            var localY = y.Type == JTokenType.Object ? modifier(y) : y;
            return base.Compare(localX, localY);
        }

        public override int GetHashCode(JToken obj)
        {
            var localObj = obj.Type == JTokenType.Object ? modifier(obj) : obj;
            return base.GetHashCode(localObj);
        }
    }
}