using System;
using System.Collections.Specialized;
using System.IO;
using System.Text;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.RavenFS.Extensions
{
	public static class ConfigurationExtension
	{
		public static T GetConfigurationValue<T>(this IStorageActionsAccessor accessor, string key)
		{
			var value = accessor.GetConfig(key)["value"];
			var serializer = new JsonSerializer
			{
				Converters = { new NameValueCollectionJsonConverter() }
			};
			return serializer.Deserialize<T>(new JsonTextReader(new StringReader(value)));
		}

		public static bool TryGetConfigurationValue<T>(this IStorageActionsAccessor accessor, string key, out T result)
		{
			try
			{
				result = GetConfigurationValue<T>(accessor, key);
				return true;
			}
			catch (FileNotFoundException)
			{
				result = default(T);
				return false;
			}
		}

		public static void SetConfigurationValue<T>(this IStorageActionsAccessor accessor, string key, T objectToSave)
		{
			var sb = new StringBuilder();
			var jw = new JsonTextWriter(new StringWriter(sb));
			var serializer = new JsonSerializer
			{
				Converters = { new NameValueCollectionJsonConverter() }
			};
			serializer.Serialize(jw, objectToSave);
			var value = sb.ToString();
			accessor.SetConfig(key, new NameValueCollection { { "value", value } });
		}

		public static NameValueCollection AsConfig<T>(this T @object) where T : class, new()
		{
			var nameValueCollection = new NameValueCollection();

			foreach (var propertyInfo in @object.GetType().GetProperties())
			{
				if (propertyInfo.CanRead)
				{
					var pName = propertyInfo.Name;
					var pValue = propertyInfo.GetValue(@object, null);
					if (pValue != null)
					{
						var propertyType = propertyInfo.PropertyType;
						if (propertyType.IsPrimitive || propertyType.IsEnum || propertyType == typeof(string) ||
							propertyType == typeof(Guid) || propertyType == typeof(DateTime))
						{
							nameValueCollection.Add(pName, pValue.ToString());
						}
						else
						{
							var serializedObject = new StringBuilder();

							new JsonSerializer
							{
								Converters = { new NameValueCollectionJsonConverter() }
							}.Serialize(new JsonTextWriter(new StringWriter(serializedObject)), pValue);

							nameValueCollection.Add(pName, serializedObject.ToString());
						}
					}
					else
					{
						nameValueCollection.Add(pName, string.Empty);
					}
				}
			}

			return nameValueCollection;
		}

		public static T AsObject<T>(this NameValueCollection config) where T : class, new()
		{
			var result = new T();

			foreach (var propertyInfo in result.GetType().GetProperties())
			{
				if (propertyInfo.CanWrite)
				{
					var pName = propertyInfo.Name;

					if (!string.IsNullOrEmpty(config[pName]))
					{
						if (propertyInfo.PropertyType.IsPrimitive)
						{
							propertyInfo.SetValue(result, Convert.ChangeType(config[pName], propertyInfo.PropertyType), null);
						}
						else if (propertyInfo.PropertyType.IsEnum)
						{
							propertyInfo.SetValue(result, Enum.Parse(propertyInfo.PropertyType, config[pName]), null);
						}
						else if (propertyInfo.PropertyType == typeof(string))
						{
							propertyInfo.SetValue(result, config[pName], null);
						}
						else if (propertyInfo.PropertyType == typeof(Guid))
						{
							propertyInfo.SetValue(result, Guid.Parse(config[pName]), null);
						}
						else if (propertyInfo.PropertyType == typeof(DateTime))
						{
							propertyInfo.SetValue(result, DateTime.Parse(config[pName]), null);
						}
						else
						{
							var deserializedObject =
								new JsonSerializer { Converters = { new NameValueCollectionJsonConverter() } }.Deserialize(
									new JsonTextReader(new StringReader(config[pName])), propertyInfo.PropertyType);

							propertyInfo.SetValue(result, deserializedObject, null);
						}
					}
				}
			}

			return result;
		}
	}
}