using System;
using System.Diagnostics;
using System.Windows.Input;
using System.Windows.Media;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Framework;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Documents
{
	public class ViewableDocument : NotifyPropertyChangedBase
	{
		private readonly JsonDocument inner;

		public ViewableDocument(JsonDocument inner)
		{
			this.inner = inner;

			Id = inner.Metadata.IfPresent<string>("@id");
			LastModified = inner.LastModified ?? DateTime.MinValue;
			if (LastModified.Kind == DateTimeKind.Utc)
				LastModified = LastModified.ToLocalTime();
			ClrType = inner.Metadata.IfPresent<string>(Constants.RavenClrType);
			CollectionType = DetermineCollectionType(inner.Metadata);
		}

		Brush fill;
		public Brush Fill
		{
			get
			{

				if(fill == null)
					fill = TemplateColorProvider.Instance.ColorFrom(CollectionType);
				return fill;
			}
		}

		public ICommand Edit
		{
			get { return new EditDocumentCommand(this); }
		}

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

		public string CollectionType
		{
			get
			{
				return collectionType;
			}
			set
			{
				collectionType = value; OnPropertyChanged();
			}
		}

		public string ClrType
		{
			get
			{
				return clrType;
			}
			set
			{
				clrType = value; OnPropertyChanged();
			}
		}

		private DateTime lastModified;
		public DateTime LastModified
		{
			get { return lastModified; }
			set { lastModified = value; OnPropertyChanged(); }
		}

		private string id;
		private string clrType;
		private string collectionType;

		public string Id
		{
			get { return id; }
			set { id = value; OnPropertyChanged(); }
		}

		public override string ToString()
		{
			return inner.DataAsJson.ToString();
		}

		public static string DetermineCollectionType(RavenJObject metadata)
		{
			var id = metadata.IfPresent<string>("@id") ?? string.Empty;

			if (string.IsNullOrEmpty(id))
				return "Projection"; // meaning that the document is a projection and not a 'real' document

			var entity = metadata.IfPresent<string>(Constants.RavenEntityName);
			if (entity != null)
				entity = entity.ToLower();
			return entity ??
				(id.StartsWith("Raven/")
					? "Sys doc"
					: "Doc");
		}
	}
}