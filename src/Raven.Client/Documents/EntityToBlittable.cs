using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Linq;
using Raven.Client.Linq;
using Raven.Client.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Imports.Newtonsoft.Json.Serialization;
using Raven.Json.Linq;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using DocumentInfo = Raven.Client.Documents.InMemoryDocumentSessionOperations.DocumentInfo;

namespace Raven.Client.Documents
{
    public class EntityToBlittable
    {
        private readonly IDocumentStore documentStore;
        private readonly JsonOperationContext _context;

        /// <summary>
        /// All the listeners for this session
        /// </summary>
        public EntityToBlittable(IDocumentStore documentStore, JsonOperationContext context)
        {
            this.documentStore = documentStore;
            _context = context;
        }
        public readonly Dictionary<object, Dictionary<string, JToken>> MissingDictionary = new Dictionary<object, Dictionary<string, JToken>>(ObjectReferenceEqualityComparer<object>.Default);

        public BlittableJsonReaderObject ConvertEntityToBlittable(string id, object entity, DocumentInfo documentInfo)
        {
           return GetObjectAsBlittable(entity, documentInfo, _context); 
        }

        private BlittableJsonReaderObject GetObjectAsBlittable(object entity, DocumentInfo documentInfo, JsonOperationContext context)
        {
            var json = documentStore.Conventions.JsonSerialize(entity, context);

            if (json.Modifications == null)
            {
                json.Modifications = new DynamicJsonValue(documentInfo.Metadata)
                {
                    [Constants.Metadata.Key] = documentInfo.Metadata
                };
            }
            else
            {
                //TODO
            }
            return context.ReadObject(json, documentInfo.Id);
        }
    }
}
