using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Blittable;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Json;
using Sparrow.Json;
using System.Runtime.Serialization.Formatters;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Commands
{
    public class CreateDatabaseCommand : RavenCommand<CreateDatabaseResult>
    {
        public BlittableJsonReaderObject DatabaseDocument;
        
        public JsonOperationContext Context;

        public override HttpRequestMessage CreateRequest(ServerNode node, out string url)
        {
            //TODO -EFRAT - WIP
            url = $"{node.Url}/admin/databases/{node.Database}";
            IsReadRequest = false;

            var request = new HttpRequestMessage
            {
                Method = HttpMethod.Put,
                Content = new BlittableJsonContent(stream =>
                {
                    Context.Write(stream, DatabaseDocument);
                })
            };
            
            return request;
        }

        public override void SetResponse(BlittableJsonReaderObject response)
        {
            if (response == null)
            {
                Result = null;
                return;
            }
        }
    }
}