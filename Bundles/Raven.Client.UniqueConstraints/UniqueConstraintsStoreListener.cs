namespace Raven.Client.UniqueConstraints
{
	using System;
	using System.Linq;
	using Listeners;
	using Json.Linq;

	using Constants = Raven.Bundles.UniqueConstraints.Constants;

	public class UniqueConstraintsStoreListener : IDocumentStoreListener
	{
		public UniqueConstraintsStoreListener()
			: this(new UniqueConstraintsTypeDictionary()) { }

		public UniqueConstraintsStoreListener(UniqueConstraintsTypeDictionary dictionary)
		{
			if (dictionary == null) { throw new ArgumentNullException("dictionary"); }

			this.UniqueConstraintsTypeDictionary = dictionary;
		}

		public UniqueConstraintsTypeDictionary UniqueConstraintsTypeDictionary { get; private set; }

		public bool BeforeStore(string key, object entityInstance, RavenJObject metadata, RavenJObject original)
		{
			if (metadata[Constants.EnsureUniqueConstraints] != null)
			{
				return true;
			}

			var type = entityInstance.GetType();

			var properties = UniqueConstraintsTypeDictionary.GetProperties(type);

			if (properties != null)
			{
                metadata.Add(Constants.EnsureUniqueConstraints, new RavenJArray(properties.Select(x =>
                    RavenJObject.FromObject(x.Configuration))));
			}

			return true;
		}

		public void AfterStore(string key, object entityInstance, RavenJObject metadata)
		{
		}
	}
}
