using System;
using System.IO;
using System.Text.RegularExpressions;
using Kayak;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Rhino.DivanDB.Server.Responders
{
    public class PutDocument : KayakResponder
    {
        public override string UrlPattern
        {
            get { return "/docs"; }
        }

        public override string[] SupportedVerbs
        {
            get { return new[] { "PUT" }; }
        }

        protected override void Respond(KayakContext context)
        {
            context.WriteJson(new { id = Database.Put(context.ReadJson()) });
        }
    }
}