using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text.RegularExpressions;
using System.Windows.Input;
using Newtonsoft.Json;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
	public class EditableDocument : NotifyPropertyChangedBase
	{
		private readonly JsonDocument document;
		private string jsonData;

		public EditableDocument(JsonDocument document)
		{
			this.document = document;
			IsProjection = string.IsNullOrEmpty(document.Key);
			References = new ObservableCollection<string>();

			JsonData = document.DataAsJson.ToString(Formatting.Indented);
			JsonMetadata = document.Metadata.ToString(Formatting.Indented);
			Metadata = document.Metadata.ToDictionary(x => x.Key, x => x.Value.ToString(Formatting.None));
		}

		public ObservableCollection<string> References { get; set; }
		public bool IsProjection { get; private set; }

		public string DisplayId
		{
			get
			{
				if (IsProjection) return "Projection";
				return string.IsNullOrEmpty(Key)
					? "New Document"
					: Key;
			}
		}

		private string jsonMetadata;
		public string JsonMetadata
		{
			get { return jsonMetadata; }
			set { jsonMetadata = value; OnPropertyChanged(); }
		}

		public string JsonData
		{
			get { return jsonData; }
			set
			{
				jsonData = value;
				UpdateReferences();
				OnPropertyChanged();
			}
		}



		private void UpdateReferences()
		{
			var referencesIds = Regex.Matches(jsonData, @"""(\w+/\w+)""");
			References.Clear();
			foreach (var source in referencesIds.Cast<Match>().Select(x => x.Groups[1].Value).Distinct())
			{
				References.Add(source);
			}

		}

		public string Key
		{
			get { return document.Key; }
			set { document.Key = value; OnPropertyChanged(); }
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

		public IDictionary<string,string> Metadata { get; private set; }

		public ICommand Save
		{
			get { return new SaveDocumentCommand(); }
		}
	}
}