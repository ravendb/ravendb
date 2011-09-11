using System;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
	public class EditableDocument : NotifyPropertyChangedBase
	{
		private readonly JsonDocument document;

		public EditableDocument(JsonDocument document)
		{
			this.document = document;
		}

		public string Key
		{
			get { return document.Key; }
			set { document.Key = value;OnPropertyChanged(); }
		}

		public Guid? Etag
		{
			get { return document.Etag; }
			set { document.Etag = value; OnPropertyChanged(); }
		}

		public DateTime? LastModified
		{
			get { return document.LastModified; }
			set { document.LastModified = value; OnPropertyChanged(); }
		}
	}
}