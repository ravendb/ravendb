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
        private static PriorityColumn[] DefaultPriorityColumns = new[]
                                                        {
                                                            new PriorityColumn() { PropertyNamePattern = "Name"},
                                                            new PriorityColumn() { PropertyNamePattern = "Title"},
                                                            new PriorityColumn() { PropertyNamePattern = "Description"},
                                                            new PriorityColumn() { PropertyNamePattern = "Status"},
                                                        };

        public ColumnSuggester()
        {
        }

        public IList<ColumnDefinition> AutoSuggest(IEnumerable<ViewableDocument> sampleDocuments, string context, IList<PriorityColumn> priorityColumns = null)
        {
            return PickLikelyColumns(sampleDocuments.Select(v => v.Document).ToArray(), context, priorityColumns);
        }

        public IList<string> AllSuggestions(IEnumerable<ViewableDocument> sampleDocuments)
        {
            return CreateSuggestedBindingsFromDocuments(sampleDocuments.Select(v => v.Document).ToArray());
        }

        private IList<string> CreateSuggestedBindingsFromDocuments(JsonDocument[] jsonDocuments)
        {
            var bindings = DocumentHelpers.GetPropertiesFromDocuments(jsonDocuments, true).Distinct()
                .Concat(DocumentHelpers.GetMetadataFromDocuments(jsonDocuments, true).Distinct().Select(b => "$Meta:" + b))
                .Concat(new[] {"$JsonDocument:ETag", "$JsonDocument:LastModified"})
                .ToArray();
            
            return bindings;
        }

        private IList<ColumnDefinition> PickLikelyColumns(JsonDocument[] sampleDocuments, string context, IList<PriorityColumn> priorityColumns)
        {
            if (priorityColumns == null || priorityColumns.Count == 0)
            {
                priorityColumns = DefaultPriorityColumns;
            }

            // only consider nested properties if any of the priority columns refers to a nested property
            var includeNestedProperties = priorityColumns.Any(c => c.PropertyNamePattern.Contains("\\."));

            var columns = DocumentHelpers.GetPropertiesFromDocuments(sampleDocuments, includeNestedProperties)
                .GroupBy(p => p)
                .Select(g => new {Property = g.Key, Occurence = g.Count()/(double) sampleDocuments.Length})
                .Select(p => new { p.Property, Importance = p.Occurence + ImportanceBoost(p.Property, context, priorityColumns) })
                .OrderByDescending(p => p.Importance)
                .ThenBy(p => p.Property)
                .Select(p => new ColumnDefinition()
                                 {
                                     Binding = p.Property, 
                                     Header = p.Property, 
                                     DefaultWidth = GetDefaultColumnWidth(p.Property, priorityColumns)
                                 })
                .Take(6)
                .ToList();

            return columns;
        }

        private string GetDefaultColumnWidth(string property, IList<PriorityColumn> priorityColumns)
        {
            var matchingColumn =
                priorityColumns.FirstOrDefault(
                    column => Regex.IsMatch(property, column.PropertyNamePattern, RegexOptions.IgnoreCase));

            return matchingColumn != null && matchingColumn.DefaultWidth.HasValue
                       ? matchingColumn.DefaultWidth.Value.ToString()
                       : "";
        }

        private double ImportanceBoost(string property, string context, IEnumerable<PriorityColumn> priorityColumns)
        {
            if (GetIndexName(context).Contains(property))
            {
                return 1;
            }
            else if (priorityColumns.Any(column => Regex.IsMatch(property, column.PropertyNamePattern, RegexOptions.IgnoreCase)))
            {
                return 0.75;
            } 
            else if (property.Contains("."))
            {
                // if the property is a nested property, and it isn't a priority column, demote it
                return -0.5;
            }
            else
            {
                return 0;
            }
        }

        private string GetIndexName(string context)
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
    }
}
