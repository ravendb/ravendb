using System;
using Newtonsoft.Json.Linq;

namespace Raven.Munin
{
    public class ModifiedJTokenComparer : JTokenComparer
    {
        private readonly Func<JToken, JToken> modifier;

        public ModifiedJTokenComparer(Func<JToken, JToken> modifier)
        {
            this.modifier = token =>
            {
                return token.Type != JTokenType.Object ? token : modifier(token);
            };
        }

        public override int Compare(JToken x, JToken y)
        {
            return base.Compare(modifier(x), modifier(y));
        }

        public override int GetHashCode(JToken obj)
        {
            return base.GetHashCode(modifier(obj));
        }
    }
}