namespace Raven.Client.UniqueConstraints
{
	using System;
	using System.Collections.Concurrent;
	using System.Linq;
	using Listeners;
	using Json.Linq;

	using Constants = Raven.Bundles.UniqueConstraints.Constants;

	public class UniqueConstraintsStoreListener : IDocumentStoreListener
	{
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
                    {
                        var att = ((UniqueConstraintAttribute) Attribute.GetCustomAttribute(x, typeof (UniqueConstraintAttribute)));
                        return RavenJObject.FromObject(new { x.Name, att.CaseInsensitive });
                    })));
			}

			return true;
		}

		public void AfterStore(string key, object entityInstance, RavenJObject metadata)
		{
		}
	}
}
