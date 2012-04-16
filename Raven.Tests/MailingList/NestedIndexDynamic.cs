using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Database.Linq;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class NestedIndexDynamic : RavenTest
	{
		public class DynamicIndex : AbstractViewGenerator
		{
			public DynamicIndex()
			{
				AddField("_");
				MapDefinitions.Add(Map);
			}

			private IEnumerable<dynamic> Map(IEnumerable<dynamic> source)
			{
				foreach (dynamic o in source)
				{
					yield return new
					{
						o.__document_id,
						_ = HandleObject(((IDynamicJsonObject)o).Inner, "")
					};
				}
			}

			private IEnumerable<IEnumerable<AbstractField>> HandleObject(RavenJObject ravenJObject, string path)
			{
				foreach (var prop in ravenJObject)
				{
					foreach (var item in HandleToken(prop.Value, path + "_" + prop.Key))
					{
						yield return item;
					}
				}
			}

			private IEnumerable<IEnumerable<AbstractField>> HandleToken(RavenJToken value, string path)
			{
				switch (value.Type)
				{
					case JTokenType.Array:
						foreach (var item in ((RavenJArray) value).SelectMany(val => HandleToken(val, path)))
						{
							yield return item;
						}
						break;
					case JTokenType.Object:
						foreach (var inner in ((RavenJObject)value))
						{
							var nestedObj = inner.Value as RavenJObject;
							if(nestedObj!=null)
							{
								foreach (var item in HandleObject(nestedObj, path + "_" + inner.Key))
								{
									yield return item;
								}
							}
							else
							{
								foreach (var item in HandleToken(inner.Value, path +"_" + inner.Key))
								{
									yield return item;
								}
							}
						}
						break;
					default:
						yield return CreateField(path, value);
						break;
				}
			}
		}

		protected override void ModifyConfiguration(Database.Config.RavenConfiguration configuration)
		{
			configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof(DynamicIndex)));
		}

		[Fact]
		public void CanQueryOnNestedPropertiesDynamically()
		{
			using (var store = NewDocumentStore())
			{

				store.DatabaseCommands.Put("test/1", null,
										   RavenJObject.Parse(
											@"
{
	""Warnings"":
	{
	   ""AccessoryWarnings"":
       [
               {
               ""Value"" : ""whatever"",
               ""Id"" : 123
               },
               {
               ""Value"" : ""dsfsdfsd sfsd sd"",
               ""Id"" : 1234
               }
       ],
 }
}"),
										   new RavenJObject());

				WaitForIndexing(store);

				var queryResult = store.DatabaseCommands.Query("DynamicIndex", new IndexQuery
				{
					Query = "_Warnings_AccessoryWarnings_Id:123"
				}, null);
				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
				Assert.Equal(1, queryResult.TotalResults);

				queryResult = store.DatabaseCommands.Query("DynamicIndex", new IndexQuery
				{
					Query = "_Warnings_AccessoryWarnings_Id:1234"
				}, null);
				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
				Assert.Equal(1, queryResult.TotalResults);
			}
		}
	}
}