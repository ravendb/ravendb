//-----------------------------------------------------------------------
// <copyright file="JTokenCloner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Munin
{
	// TODO: Why not use RavenJToken.Clone?!!!
    public class RavenJTokenCloner
    {
		public static RavenJToken Clone(RavenJToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Object:
                case JTokenType.Array:
                case JTokenType.Integer:
                case JTokenType.Float:
                case JTokenType.String:
                case JTokenType.Boolean:
                case JTokenType.Date:
                case JTokenType.Null:
                case JTokenType.Bytes:
					return token.CloneToken();
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}