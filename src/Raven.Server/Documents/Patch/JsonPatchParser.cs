using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.JsonPatch;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema;

namespace Raven.Server.Documents.Patch
{
    public class JsonPatchParser
    {
        public void ParseJsonPatch(string jsonPatchCommand)
        {
            
            
        }

        class JsonPatch
        {
            private string operation;
            private string path;
            private string value;
        }
    }
}
