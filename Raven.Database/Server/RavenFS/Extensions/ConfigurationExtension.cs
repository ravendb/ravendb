using System;
using System.Collections.Specialized;
using System.IO;
using System.Text;
using System.Linq;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Database.Server.RavenFS.Extensions
{
	public static class ConfigurationExtension
	{
        public static T GetConfigurationValue<T>(this IStorageActionsAccessor accessor, string key)
        {
            var value = accessor.GetConfig(key);
            if (typeof(T).IsValueType || typeof(T) == typeof(string))
                return value.Value<T>("Value");

            return JsonExtensions.JsonDeserialization<T>(value);
        }

        public static IEnumerable<T> GetConfigurationValuesStartWithPrefix<T>(this IStorageActionsAccessor accessor, string prefix, int start, int take)
        {
            var values = accessor.GetConfigsStartWithPrefix(prefix, start, take);
            if (typeof(T).IsValueType || typeof(T) == typeof(string))
            {
                return values.Select(x => x.Value<T>("Value"));
            }

            return values.Select(x => JsonExtensions.JsonDeserialization<T>(x));
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
            accessor.SetConfig(key, JsonExtensions.ToJObject(objectToSave));
        }




        //[Obsolete("This method is only intended as an stop-gap while getting rid of all NameValueCollection objects in RavenFS.")]
        //public static NameValueCollection AsConfig<T>(this T @object) where T : class, new()
        //{
        //    var nameValueCollection = new NameValueCollection();

        //    foreach (var propertyInfo in @object.GetType().GetProperties())
        //    {
        //        if (propertyInfo.CanRead)
        //        {
        //            var pName = propertyInfo.Name;
        //            var pValue = propertyInfo.GetValue(@object, null);
        //            if (pValue != null)
        //            {
        //                var propertyType = propertyInfo.PropertyType;
        //                if (propertyType.IsPrimitive || propertyType.IsEnum || propertyType == typeof(string) ||
        //                    propertyType == typeof(Guid) || propertyType == typeof(DateTime))
        //                {
        //                    nameValueCollection.Add(pName, pValue.ToString());
        //                }
        //                else
        //                {
        //                    var serializedObject = new StringBuilder();

        //                    new JsonSerializer
        //                    {
        //                        Converters = { new NameValueCollectionJsonConverter() }
        //                    }.Serialize(new JsonTextWriter(new StringWriter(serializedObject)), pValue);

        //                    nameValueCollection.Add(pName, serializedObject.ToString());
        //                }
        //            }
        //            else
        //            {
        //                nameValueCollection.Add(pName, string.Empty);
        //            }
        //        }
        //    }

        //    return nameValueCollection;
        //}

        [Obsolete("This method is only intended as an stop-gap while getting rid of all NameValueCollection objects in RavenFS.")]
        public static NameValueCollection ToNameValueCollection(this RavenJObject @object)
        {
            var result = new NameValueCollection();            

            foreach( var item in @object )
            {                
                if ( item.Value is RavenJArray )
                {
                    var array = item.Value as RavenJArray;
                    foreach (var itemInArray in array)
                    {
                        result.Add(item.Key, itemInArray.ToString());
                    }
                }
                else if ( item.Value is RavenJValue )
                {
                    result.Add(item.Key, item.Value.ToString());
                }
            }

            return result;
        }

        [Obsolete("This method is only intended as an stop-gap while getting rid of all NameValueCollection objects in RavenFS.")]
        public static RavenJObject ToJObject(this NameValueCollection config)
        {
            var serializedObject = new StringBuilder();

            new JsonSerializer
            {
                Converters = { new NameValueCollectionJsonConverter() }
            }
            .Serialize(new JsonTextWriter(new StringWriter(serializedObject)), config);

            return RavenJObject.Parse(serializedObject.ToString());
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