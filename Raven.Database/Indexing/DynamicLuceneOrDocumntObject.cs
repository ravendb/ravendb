// -----------------------------------------------------------------------
//  <copyright file="DynamicLuceneOrDocumntObject.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Dynamic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Linq;
using Raven.Database.Impl;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using System.Linq;

namespace Raven.Database.Indexing
{
	[JsonObject]
    public class DynamicLuceneOrParentDocumntObject : DynamicJsonObject
    {
        private readonly DocumentRetriever retriever;
        private dynamic parentDoc;

        public DynamicLuceneOrParentDocumntObject(DocumentRetriever retriever,RavenJObject inner) : base(inner)
        {
            this.retriever = retriever;
        }

        public override void WriteTo(JsonWriter writer)
        {
            var dynamicJsonObject = parentDoc as IDynamicJsonObject;
            if (dynamicJsonObject != null)
            {
                dynamicJsonObject.WriteTo(writer);
            }
            else
            {
                base.WriteTo(writer);
            }
        }

        public override object GetValue(string name)
        {
            var result = base.GetValue(name);
           
            if (name == Constants.Metadata)
            {
                if (parentDoc == null)
                {
                    if (TryLoadParentDoc() == false)
                        return new DynamicNullObject();
                }
                var metadata = parentDoc[name].Inner as RavenJObject;
                var indexMetadata = result as IDynamicJsonObject;
                if (metadata == null || indexMetadata == null)
                    return parentDoc[name];

                foreach (var item in indexMetadata.Inner)
                {
                    metadata[item.Key] = item.Value;
                }
                return metadata;
            }

            if (result is DynamicNullObject == false)
                return result;

            if (parentDoc != null)
                return parentDoc[name];

            if (TryLoadParentDoc() == false) 
                return result;

            return parentDoc[name];
        }

	    private bool TryLoadParentDoc()
	    {
	        object documentId = GetDocumentId() as string;
	        if (documentId == null)
	        {
	            return false;
	        }

	        parentDoc = retriever.Load(documentId);
	        return true;
	    }
    }
}