//-----------------------------------------------------------------------
// <copyright file="DynamicQueryMapping.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Database.Server;

namespace Raven.Database.Data
{
	public class DynamicQueryMapping
	{
		public bool DynamicAggregation { get; set; }
		public string IndexName { get; set; }
		public string ForEntityName { get; set; }
		public DynamicSortInfo[] SortDescriptors { get; set; }
		public DynamicQueryMappingItem[] Items { get; set; }
		public AggregationOperation AggregationOperation { get; set; }
		public string TemporaryIndexName { get; set; }
		public string PermanentIndexName { get; set; }
		protected DynamicQueryMappingItem[] GroupByItems { get; set; }

		public DynamicQueryMapping()
		{
			Items = new DynamicQueryMappingItem[0];
			SortDescriptors = new DynamicSortInfo[0];
		}

		public IndexDefinition CreateIndexDefinition()
		{
			var fromClauses = new HashSet<string>();
			var realMappings = new HashSet<string>();

			if (!string.IsNullOrEmpty(ForEntityName))
			{
				fromClauses.Add("from doc in docs." + ForEntityName);
			}
			else
			{
				fromClauses.Add("from doc in docs");
			}

			foreach (var map in Items)
			{
				var currentDoc = "doc";
				var currentExpression = new StringBuilder();

				int currentIndex = 0;
				while (currentIndex < map.From.Length)
				{
					char currentChar = map.From[currentIndex++];
					switch (currentChar)
					{
						case ',':

							// doc.NewDoc.Items
							String newDocumentSource = string.Format("{0}.{1}", currentDoc, currentExpression);

							// docNewDocItemsItem
							String newDoc = string.Format("{0}Item", newDocumentSource.Replace(".", ""));

							// from docNewDocItemsItem in doc.NewDoc.Items
							String docInclude = string.Format("from {0} in ((IEnumerable<dynamic>){1}).DefaultIfEmpty()", newDoc, newDocumentSource);
							fromClauses.Add(docInclude);

							// Start building the property again
							currentExpression.Clear();

							// And from this new doc
							currentDoc = newDoc;

							break;
						default:
							currentExpression.Append(currentChar);
							break;
					}
				}

				if (currentExpression.Length > 0 && currentExpression[0] != '[')
				{
					currentExpression.Insert(0, '.');
				}
				// We get rid of any _Range(s) etc
				var indexedMember = currentExpression.ToString().Replace("_Range", "");
				if (indexedMember.Length == 0)
				{
					realMappings.Add(string.Format("{0} = {1}",
						map.To.Replace("_Range", ""),
						currentDoc
						));
				}
				else
				{
					realMappings.Add(string.Format("{0} = {1}{2}",
						map.To.Replace("_Range", ""),
						currentDoc,
						indexedMember
						));
				}
			}

			var index = new IndexDefinition
			{
				Map = string.Format("{0}\r\nselect new {{ {1} }}",
									string.Join("\r\n", fromClauses.ToArray()),
									string.Join(", ",
												realMappings.Concat(new[] { AggregationMapPart() }).Where(x => x != null))),
				Reduce = DynamicAggregation ? null : AggregationReducePart(),
				TransformResults = DynamicAggregation ? AggregationReducePart() : null,
			};

			if (DynamicAggregation)
			{
				foreach (var item in GroupByItems)
				{
					index.Stores[ToFieldName(item.To)] = FieldStorage.Yes;
				}
			}

			foreach (var descriptor in SortDescriptors)
			{
				index.SortOptions[ToFieldName(descriptor.Field)] = descriptor.FieldType;
			}
			return index;
		}

		private string ToFieldName(string field)
		{
			var item = Items.FirstOrDefault(x => x.From == field);
			if (item == null)
				return field;
			return item.To;
		}

		private string AggregationReducePart()
		{
			switch (AggregationOperation)
			{
				case AggregationOperation.None:
					return null;
				case AggregationOperation.Count:
					{
						var sb = new StringBuilder()
							.AppendLine("from result in results")
							.Append("group result by ");

						AppendGroupByClauseForReduce(sb);

						sb.AppendLine("into g");

						sb.AppendLine("select new")
							.AppendLine("{");

						AppendSelectClauseForReduce(sb);


						if (DynamicAggregation == false)
							sb.AppendLine("\tCount = g.Sum(x=>x.Count)");
						else
							sb.AppendLine("\tCount = g.Count()");

						sb.AppendLine("}");

						return sb.ToString();
					}
				default:
					throw new InvalidOperationException("Unknown AggregationOperation option: " + AggregationOperation);
			}
		}

		private void AppendSelectClauseForReduce(StringBuilder sb)
		{
			var groupByItemsSource = DynamicAggregation ? GroupByItems : Items;
			if (groupByItemsSource.Length == 1)
			{
				sb.Append("\t").Append(groupByItemsSource[0].To).AppendLine(" = g.Key,");
			}
			else
			{
				foreach (var item in groupByItemsSource)
				{
					sb.Append("\t").Append(item.To).Append(" = ").Append(" g.Key.").Append(item.To).
						AppendLine(",");
				}
			}
		}

		private void AppendGroupByClauseForReduce(StringBuilder sb)
		{
			var groupBySourceItems = DynamicAggregation ? GroupByItems : Items;
			if (groupBySourceItems.Length == 1)
			{
				sb.Append("result.").Append(groupBySourceItems[0].To);
			}
			else
			{
				sb.AppendFormat("new {{ {0} }}", string.Join(", ", groupBySourceItems.Select(x => "result." + x.To)));
			}
			sb.AppendLine();
		}

		private string AggregationMapPart()
		{
			if (DynamicAggregation)
				return null;
			switch (AggregationOperation)
			{
				case AggregationOperation.None:
					return null;
				case AggregationOperation.Count:
					return "Count = 1";
				default:
					throw new InvalidOperationException("Unknown AggregationOperation option: " + AggregationOperation);
			}
		}

		public static DynamicQueryMapping Create(DocumentDatabase database, string query, string entityName)
		{
			return Create(database, new IndexQuery
			{
				Query = query
			}, entityName);
		}

		public static DynamicQueryMapping Create(DocumentDatabase database, IndexQuery query, string entityName)
		{
			var fields = SimpleQueryParser.GetFieldsForDynamicQuery(query);

			if (query.SortedFields != null)
			{
				foreach (var sortedField in query.SortedFields)
				{
					if (sortedField.Field.StartsWith(Constants.RandomFieldName))
						continue;
					if (sortedField.Field == Constants.TemporaryScoreValue)
						continue;
					fields.Add(Tuple.Create(sortedField.Field, sortedField.Field));
				}
			}

			var dynamicQueryMapping = new DynamicQueryMapping
			{
				AggregationOperation = query.AggregationOperation.RemoveOptionals(),
				DynamicAggregation = query.AggregationOperation.HasFlag(AggregationOperation.Dynamic),
				ForEntityName = entityName,
				SortDescriptors = GetSortInfo(fieldName =>
				{
					if (fields.Any(x => x.Item2 == fieldName || x.Item2 == (fieldName + "_Range")) == false)
						fields.Add(Tuple.Create(fieldName, fieldName));
				})
			};
			dynamicQueryMapping.SetupFieldsToIndex(query, fields);
			dynamicQueryMapping.SetupSortDescriptors(dynamicQueryMapping.SortDescriptors);
			dynamicQueryMapping.FindIndexName(database, dynamicQueryMapping, query);
			return dynamicQueryMapping;
		}

		private void SetupSortDescriptors(DynamicSortInfo[] sortDescriptors)
		{
			foreach (var dynamicSortInfo in sortDescriptors)
			{
				dynamicSortInfo.Field = ReplaceIndavlidCharactersForFields(dynamicSortInfo.Field);
			}
		}

		static readonly Regex replaceInvalidCharacterForFields = new Regex(@"[^\w_]", RegexOptions.Compiled);
		private void SetupFieldsToIndex(IndexQuery query, IEnumerable<Tuple<string, string>> fields)
		{
			if (query.GroupBy != null && query.GroupBy.Length > 0)
			{
				GroupByItems = query.GroupBy.Select(x => new DynamicQueryMappingItem
				{
					From = x,
					To = x.Replace(".", "").Replace(",", ""),
					QueryFrom = x
				}).ToArray();
			}
			if (DynamicAggregation == false &&
				AggregationOperation != AggregationOperation.None &&
				query.GroupBy != null && query.GroupBy.Length > 0)
			{
				Items = GroupByItems;
			}
			else
			{
				Items = fields.Select(x => new DynamicQueryMappingItem
				{
					From = x.Item1,
					To = ReplaceIndavlidCharactersForFields(x.Item2),
					QueryFrom = x.Item2
				}).OrderByDescending(x => x.QueryFrom.Length).ToArray();
				if (GroupByItems != null && DynamicAggregation)
				{
					Items = Items.Concat(GroupByItems).OrderByDescending(x => x.QueryFrom.Length).ToArray();
					var groupBys = GroupByItems.Select(x => x.To).ToArray();
					query.FieldsToFetch = query.FieldsToFetch == null ?
						groupBys :
						query.FieldsToFetch.Concat(groupBys).ToArray();
				}
			}
		}

		public static string ReplaceIndavlidCharactersForFields(string field)
		{
			return replaceInvalidCharacterForFields.Replace(field, "_");
		}

		public static DynamicSortInfo[] GetSortInfo(Action<string> addField)
		{
			var headers = CurrentOperationContext.Headers.Value;
			var sortInfo = new List<DynamicSortInfo>();
			String[] sortHintHeaders = headers.AllKeys
				.Where(key => key.StartsWith("SortHint")).ToArray();
			foreach (string sortHintHeader in sortHintHeaders)
			{
				String[] split = sortHintHeader.Split(sortHintHeader.Contains("-") ? '-' : '_'); // we only use _ for backward compatibility
				String fieldName = Uri.UnescapeDataString(split[1]);
				if (fieldName == Constants.TemporaryScoreValue)
					continue;

				if (fieldName.EndsWith("_Range"))
					fieldName = fieldName.Substring(0, fieldName.Length - "_Range".Length);
				string fieldType = headers[sortHintHeader];

				sortInfo.Add(new DynamicSortInfo
				{
					Field = fieldName,
					FieldType = (SortOptions)Enum.Parse(typeof(SortOptions), fieldType)
				});

				addField(fieldName);
			}
			return sortInfo.ToArray();
		}

		private void FindIndexName(DocumentDatabase database, DynamicQueryMapping map, IndexQuery query)
		{
			var targetName = map.ForEntityName ?? "AllDocs";

			var combinedFields = String.Join("And",
				map.Items
				.OrderBy(x => x.To)
				.Select(x => x.To));
			var indexName = combinedFields;

			if (map.SortDescriptors != null && map.SortDescriptors.Length > 0)
			{
				indexName = string.Format("{0}SortBy{1}", indexName,
										  String.Join("",
													  map.SortDescriptors
														  .Select(x => x.Field)
														  .OrderBy(x => x)));
			}
			string groupBy = null;
			if (AggregationOperation != AggregationOperation.None)
			{
				if (query.GroupBy != null && query.GroupBy.Length > 0)
				{
					groupBy += "/" + AggregationOperation + "By" + string.Join("And", query.GroupBy);
				}
				else
				{
					groupBy += "/" + AggregationOperation;
				}
				if (DynamicAggregation)
					groupBy += "Dynamically";
			}

			if (database.Configuration.RunInUnreliableYetFastModeThatIsNotSuitableForProduction == false &&
				database.Configuration.RunInMemory == false)
			{
				// Hash the name if it's too long (as a path)
				if ((database.Configuration.DataDirectory.Length + indexName.Length) > 230)
				{
					using (var sha256 = SHA256.Create())
					{
						var bytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(indexName));
						indexName = Convert.ToBase64String(bytes);
					}
				}
			}

			var permanentIndexName = indexName.Length == 0
					? string.Format("Auto/{0}{1}", targetName, groupBy)
					: string.Format("Auto/{0}/By{1}{2}", targetName, indexName, groupBy);

			var temporaryIndexName = indexName.Length == 0
					? string.Format("Temp/{0}{1}", targetName, groupBy)
					: string.Format("Temp/{0}/By{1}{2}", targetName, indexName, groupBy);


			// If there is a permanent index, then use that without bothering anything else
			var permanentIndex = database.GetIndexDefinition(permanentIndexName);
			map.PermanentIndexName = permanentIndexName;
			map.TemporaryIndexName = temporaryIndexName;
			map.IndexName = permanentIndex != null ? permanentIndexName : temporaryIndexName;
		}

		public class DynamicSortInfo
		{
			public string Field { get; set; }
			public SortOptions FieldType { get; set; }
		}
	}
}
