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

        public Task<IList<string>> AutoSuggest()
        {
            return GetSampleDocuments().ContinueWith(t => PickLikelyColumns(t.Result));
        }

        public Task<IList<string>> AllSuggestions()
        {
            return GetSampleDocuments().ContinueWith(t => CreateSuggestedBindingsFromDocuments(t.Result));
        }

        private IEnumerable<string> GetPropertiesFromDocuments(JsonDocument[] jsonDocuments, bool includeNestedPropeties)
        {
            return
                jsonDocuments.SelectMany(
                    doc =>
                    GetPropertiesFromJObject(doc.DataAsJson, parentPropertyPath: "",
                                             includeNestedProperties: includeNestedPropeties));

        }

        private IEnumerable<string> GetMetadataFromDocuments(JsonDocument[] jsonDocuments, bool includeNestedPropeties)
        {
            return
                jsonDocuments.SelectMany(
                    doc =>
                    GetPropertiesFromJObject(doc.Metadata, parentPropertyPath: "",
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

        private IList<string> CreateSuggestedBindingsFromDocuments(JsonDocument[] jsonDocuments)
        {
            var bindings = GetPropertiesFromDocuments(jsonDocuments, true).Distinct()
                .Concat(GetMetadataFromDocuments(jsonDocuments, true).Distinct().Select(b => "$Meta:" + b))
                .Concat(new[] {"$JsonDocument:ETag", "$JsonDocument:LastModified"})
                .ToArray();
            
            return bindings;
        }

        private IList<string> PickLikelyColumns(JsonDocument[] sampleDocuments)
        {
            var columns= GetPropertiesFromDocuments(sampleDocuments, includeNestedPropeties: false)
                .GroupBy(p => p)
                .Select(g => new {Property = g.Key, Occurence = g.Count()/(double) sampleDocuments.Length})
                .Select(p => new {p.Property, Occurence = p.Occurence + ImportanceBoost(p.Property)})
                .OrderByDescending(p => p.Occurence)
                .Select(p => p.Property)
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

        private Task<JsonDocument[]> GetSampleDocuments()
        {
            var sampleSize = 10;
            return source.GetPageAsync(0, 10, null)
                          .ContinueWith(t => t.Result.Select(d => d.Document).ToArray());
        }
    }
}
