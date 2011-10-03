using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;

namespace RavenView
{
    [JsonObject(IsReference = true)] 
    public class TestClass
    {
        public string Id { get; set; }
        public List<Item> Items { get; set; }
    }
}
