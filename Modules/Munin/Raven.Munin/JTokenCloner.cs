//-----------------------------------------------------------------------
// <copyright file="JTokenCloner.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;

namespace Raven.Munin
{
    public class JTokenCloner
    {
        public static JToken Clone(JToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Object:
                    return new JObject((JObject)token);
                case JTokenType.Array:
                    return new JArray((JArray) token);
                case JTokenType.Integer:
                case JTokenType.Float:
                case JTokenType.String:
                case JTokenType.Boolean:
                case JTokenType.Date:
                case JTokenType.Null:
                case JTokenType.Bytes:
                    return new JValue(((JValue)token).Value);
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}