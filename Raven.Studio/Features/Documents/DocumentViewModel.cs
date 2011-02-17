namespace Raven.Studio.Features.Documents
{
	using System;
	using Controls;
	using Framework;
	using Newtonsoft.Json.Linq;
	using Raven.Database;

	/// <summary>
	/// This view model is for displaying documents in bulk. There is no change notification and no behaviours related to editing
	/// </summary>
	public class DocumentViewModel : ISupportDocumentTemplate
	{
		const int SummaryLength = 150;
		readonly JsonDocument inner;

		public DocumentViewModel(JsonDocument inner)
		{
			this.inner = inner;
			Id = inner.Metadata.IfPresent<string>("@id");
			LastModified = inner.Metadata.IfPresent<DateTime>("Last-Modified");
			ClrType = inner.Metadata.IfPresent<string>("Raven-Clr-Type");
			CollectionType = DetermineCollectionType();
		}

		public string Id { get; private set; }
		public string ClrType { get; private set; }
		public string CollectionType { get; private set; }
		public DateTime LastModified { get; private set; }

		public JObject Contents
		{
			get { return inner.DataAsJson; }
		}

		public string Summary
		{
			get
			{
				var json = inner.DataAsJson.ToString();
				json = (json.Length > SummaryLength)
				       	? json.Substring(0, SummaryLength) + "..."
				       	: json;

				return json
					.Replace("\r", "")
					.Replace("\n", " ")
					.TrimStart('{', ' ')
					.TrimEnd('}');
			}
		}

		public JObject Metadata
		{
			get { return inner.Metadata; }
		}

		string DetermineCollectionType()
		{
			return Metadata.IfPresent<string>("Raven-Entity-Name") ?? (Id.StartsWith("Raven/") ? "System" : null) ?? "document";
		}

		string ISupportDocumentTemplate.TemplateKey
		{
			get { return CollectionType; }
		}
	}
}