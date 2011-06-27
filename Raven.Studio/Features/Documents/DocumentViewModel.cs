using Caliburn.Micro;
using Raven.Json.Linq;
using Raven.Studio.Framework;
using Raven.Studio.Messages;

namespace Raven.Studio.Features.Documents
{
    using System;
    using Collections;
	using Abstractions.Data;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// This view model is for displaying documents in bulk. There is no change notification and no behaviors related to editing
    /// </summary>
    public class DocumentViewModel : ISupportDocumentTemplate,
		IHandle<DocumentUpdated>
    {
        const int SummaryLength = 150;
        JsonDocument inner;

        public DocumentViewModel(JsonDocument inner)
        {
        	SetFromJsonDocument(inner);
        }

		private void SetFromJsonDocument(JsonDocument inner)
    	{
			this.inner = inner;
			Id = inner.Metadata.IfPresent<string>("@id");
			LastModified = inner.LastModified ?? DateTime.MinValue;
			if (LastModified.Kind == DateTimeKind.Utc)
				LastModified = LastModified.ToLocalTime();
			ClrType = inner.Metadata.IfPresent<string>(Raven.Abstractions.Data.Constants.RavenClrType);
			CollectionType = DetermineCollectionType();
    	}

    	public string Id { get; private set; }
        public string ClrType { get; private set; }
        public string CollectionType { get; private set; }
        public DateTime LastModified { get; private set; }

        public string DisplayId
        {
            get
            {
                if (string.IsNullOrEmpty(Id)) return string.Empty;

            	var display = GetIdWithoutPrefixes();

            	Guid guid;
                if (Guid.TryParse(display, out guid))
                {
                    display = display.Substring(0, 8);
                }
                return display;
            }
        }

    	private string GetIdWithoutPrefixes()
    	{
    		var display = Id;

    		var prefixToRemoves = new[]
    		{
    			"Raven/",
    			CollectionType + "/",
    			CollectionType + "-"
    		};

    		foreach (var prefixToRemove in prefixToRemoves)
    		{
    			if (display.StartsWith(prefixToRemove, StringComparison.InvariantCultureIgnoreCase))
    				display = display.Substring(prefixToRemove.Length);
    		}
    		return display;
    	}

    	public RavenJObject Contents
        {
            get { return JsonDocument.DataAsJson; }
        }

        public string this[string path]
        {
            get
            {
                RavenJToken selectToken = JsonDocument.DataAsJson.SelectToken(path);
                if (selectToken == null || 
                    selectToken.Type == JTokenType.Null || 
                    selectToken.Type == JTokenType.Undefined)
                    return null;
                if (selectToken.Type == JTokenType.Object ||
                    selectToken.Type == JTokenType.Array)
                    return selectToken.ToString(Formatting.Indented);
                return ((RavenJValue)selectToken).Value.ToString();
            }
        }

        public string Summary
        {
            get
            {
                var json = JsonDocument.DataAsJson.ToString();
                json = (json.Length > SummaryLength)
                        ? json.Substring(0, SummaryLength) + "..." + Environment.NewLine + "}"
                        : json;

                return json;
            }
        }

        public RavenJObject Metadata
        {
            get { return JsonDocument.Metadata; }
        }

        public JsonDocument JsonDocument
        {
            get { return inner; }
        }

        string ISupportDocumentTemplate.TemplateKey
        {
            get { return CollectionType; }
        }

        string DetermineCollectionType()
        {
            return DetermineCollectionType(Metadata);
        }

        public static string DetermineCollectionType(RavenJObject metadata)
        {
            var id = metadata.IfPresent<string>("@id") ?? string.Empty;

            if (string.IsNullOrEmpty(id))
                return BuiltinCollectionName.Projection; // meaning that the document is a projection and not a 'real' document

            var entity = metadata.IfPresent<string>(Constants.RavenEntityName);
			if (entity != null)
				entity = entity.ToLower();
            return entity ??
                (id.StartsWith("Raven/")
                    ? BuiltinCollectionName.System
                    : BuiltinCollectionName.Document);
        }

    	public void Handle(DocumentUpdated message)
    	{
			SetFromJsonDocument(message.Document.JsonDocument);
    	}
    }
}