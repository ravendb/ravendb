using System;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;

namespace Raven.Abstractions.Json
{
    /// <summary>
    /// Extensions for JToken
    /// </summary>
    public static class JTokenExtensions
    {
        public static IEnumerable<JToken> SelectTokenWithRavenSyntax(this JToken self, string path)
        {
            var pathParts = path.Split(new[]{','}, 2,StringSplitOptions.RemoveEmptyEntries);
            var result = self.SelectToken(pathParts[0]);
            if(pathParts.Length == 1)
            {
                yield return result;
                yield break;
            }
            if(result == null)
            {
                yield break;
            }
            foreach (var item in result)
            {
                foreach (var subItem in item.SelectTokenWithRavenSyntax(pathParts[1]))
                {
                    yield return subItem;
                }
            }
        }
    }
}