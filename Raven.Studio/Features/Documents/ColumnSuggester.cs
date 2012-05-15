using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.Documents
{
    public class ColumnSuggester
    {
        private readonly IVirtualCollectionSource<ViewableDocument> source;
        private readonly string context;
        private static string[] ImportantProperties = new[]
                                                        {
                                                            "Name",
                                                            "Title",
                                                            "Description",
                                                            "Status"
                                                        };

        public ColumnSuggester(IVirtualCollectionSource<ViewableDocument> source, string context)
        {
            this.source = source;
            this.context = context;
        }

        private IEnumerable<string> GetPropertiesFromDocuments(JsonDocument[] jsonDocuments, bool includeNestedPropeties)
        {
            return
                jsonDocuments.SelectMany(
                    doc =>
                    GetPropertiesFromJObject(doc.DataAsJson, parentPropertyPath: "",
                                             includeNestedProperties: includeNestedPropeties));

        }

        private IEnumerable<string> GetPropertiesFromJObject(RavenJObject jObject, string parentPropertyPath, bool includeNestedProperties)
        {
            var properties = from property in jObject
                             select
                                 new
                                     {
                                         Path = parentPropertyPath + (string.IsNullOrEmpty(parentPropertyPath) ? "" : ".") +
                                         property.Key,
                                         property.Value
                                     };

            foreach (var property in properties)
            {
                yield return property.Path;

                if (includeNestedProperties && property.Value is RavenJObject)
                {
                    foreach (var childProperty in GetPropertiesFromJObject(property.Value as RavenJObject, property.Path, true))
                    {
                        yield return childProperty;
                    }
                }
            }
        }


        private IList<SuggestedColumn> CreateSuggestedColumnsFromDocuments(JsonDocument[] jsonDocuments, bool includeNestedPropeties)
        {
            var columnsByBinding = new Dictionary<string, SuggestedColumn>();
            var foundColumns = jsonDocuments.SelectMany(jDoc => CreateSuggestedColumnsFromJObject(jDoc.DataAsJson, parentPropertyPath: "", includeNestedProperties: includeNestedPropeties));

            foreach (var suggestedColumn in foundColumns)
            {
                if (!columnsByBinding.ContainsKey(suggestedColumn.Binding))
                {
                    columnsByBinding.Add(suggestedColumn.Binding, suggestedColumn);
                }
                else
                {
                    var existingColumn = columnsByBinding[suggestedColumn.Binding];
                    existingColumn.MergeFrom(suggestedColumn);
                }
            }

            return columnsByBinding.OrderBy(kv => kv.Key).Select(kv => kv.Value).ToList();
        }

        private IList<SuggestedColumn> CreateSuggestedColumnsFromJObject(RavenJObject jObject, string parentPropertyPath, bool includeNestedProperties)
        {
            return (from property in jObject
                    let path = parentPropertyPath + (string.IsNullOrEmpty(parentPropertyPath) ? "" : ".") + property.Key
                    select new SuggestedColumn()
                    {
                        Header = path,
                        Binding = path,
                        Children = property.Value is RavenJObject && includeNestedProperties 
                                ?  CreateSuggestedColumnsFromJObject(property.Value as RavenJObject, path, includeNestedProperties)
                                : new SuggestedColumn[0]
                    }).ToList();
        }

        public Task<IList<SuggestedColumn>> AutoSuggest()
        {
            return GetSampleDocuments().ContinueWith(t => PickLikelyColumns(t.Result));
        }

        private IList<SuggestedColumn> PickLikelyColumns(JsonDocument[] sampleDocuments)
        {
            var columns= GetPropertiesFromDocuments(sampleDocuments, includeNestedPropeties: false)
                .GroupBy(p => p)
                .Select(g => new {Property = g.Key, Occurence = g.Count()/(double) sampleDocuments.Length})
                .Select(p => new {p.Property, Occurence = p.Occurence + ImportanceBoost(p.Property)})
                .OrderByDescending(p => p.Occurence)
                .Select(p => new SuggestedColumn() {Binding = p.Property, Header = p.Property})
                .Take(6)
                .ToList();

            return columns;
        }

        private double ImportanceBoost(string property)
        {
            if (GetIndexName().Contains(property))
            {
                return 1;
            }
            else
            {
                return ImportantProperties.Any(importantProperty => Regex.IsMatch(property, importantProperty, RegexOptions.IgnoreCase)) ? 0.5 : 0;
            }
        }

        private string GetIndexName()
        {
            if (context.StartsWith("Index/"))
            {
                return context.Substring("Index/".Length);
            }
            else
            {
                return "";
            }
        }

        public Task<IList<SuggestedColumn>> AllSuggestions()
        {
            return AllSuggestions(includeNestedProperties:true);
        }

        private Task<IList<SuggestedColumn>> AllSuggestions(bool includeNestedProperties)
        {
            return GetSampleDocuments().ContinueWith(t => CreateSuggestedColumnsFromDocuments(t.Result, includeNestedProperties));
        }

        private Task<JsonDocument[]> GetSampleDocuments()
        {
            var sampleSize = 10;
            return source.GetPageAsync(0, 10, null)
                          .ContinueWith(t => t.Result.Select(d => d.Document).ToArray());
        }
    }
}
