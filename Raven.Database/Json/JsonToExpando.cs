using System;
using System.Collections.Generic;
using System.Linq;
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