// -----------------------------------------------------------------------
//  <copyright file="ExecuteQueryCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reactive.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Linq;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Client.Extensions;
using Raven.Studio.Extensions;

namespace Raven.Studio.Features.Query
{
	public class ExecuteQueryCommand : Command
	{
		private readonly QueryModel model;
		private string query;

		public ExecuteQueryCommand(QueryModel model)
		{
			this.model = model;
		}

		public override void Execute(object parameter)
		{
			query = model.Query;
			ClearRecentQuery();
			model.RememberHistory();

			Observable.FromEventPattern<VirtualCollectionSourceChangedEventArgs>(
				h => model.CollectionSource.CollectionChanged += h, h => model.CollectionSource.CollectionChanged -= h)
				.Where(p => p.EventArgs.ChangeType == ChangeType.Refresh)
				.Take(1)
				.ObserveOnDispatcher()
				.Subscribe(_ =>
							   {
								   if (model.CollectionSource.Count == 0)
								   {
									   SuggestResults();
								   }
							   });

            model.DocumentsResult.SetPriorityColumns(GetRelevantFields());
		    var templateQuery = CreateTemplateQuery();
		    model.QueryUrl = templateQuery.GetIndexQueryUrl("", model.IndexName, "indexes");
			var url = ApplicationModel.Current.Server.Value.Url;
			if (url.EndsWith("/") == false)
				url += "/";
			if (model.Database.Value.Name != Constants.SystemDatabase)
				url += "databases/" + model.Database.Value.Name;
			model.FullQueryUrl = templateQuery.GetIndexQueryUrl(url, model.IndexName, "indexes");
		    model.CollectionSource.UpdateQuery(model.IndexName, templateQuery);  
		}

	    private IList<string> GetRelevantFields()
	    {
	        return model.QueryDocument.GetTextOfAllTokensMatchingType("Field").Select(t => t.TrimEnd(':').Trim())
	            .Concat(
	                model.SortBy
                    .Where(s => !string.IsNullOrEmpty(s.Value))
                    .Select(s => s.Value.EndsWith("DESC") ? s.Value.Substring(0, s.Value.Length - 5) : s.Value))
	            .Distinct()
                .SelectMany(f => f.Contains("_") ? new[] {f, f.Replace("_", ".")} : new[] { f}) // if the field name contains underscores, it is likely referring to a property 
	            .ToList();
	    }

		private void ClearRecentQuery()
		{
			model.ClearQueryError();
			model.Suggestions.Clear();
		}

		private IndexQuery CreateTemplateQuery()
		{
			var q = new IndexQuery
						{
							Query = query,
							DefaultOperator = model.DefualtOperator
						};

			if (model.SortBy != null && model.SortBy.Count > 0)
			{
				var sortedFields = new List<SortedField>();
				foreach (var sortByRef in model.SortBy)
				{
					var sortBy = sortByRef.Value;
					if (sortBy.EndsWith(QueryModel.SortByDescSuffix))
					{
						var field = sortBy.Remove(sortBy.Length - QueryModel.SortByDescSuffix.Length);
						sortedFields.Add(new SortedField(field) {Descending = true});
					}
					else
						sortedFields.Add(new SortedField(sortBy));
				}
				q.SortedFields = sortedFields.ToArray();
			}

			if (model.ShowFields)
			{
				q.FieldsToFetch = new[] { Constants.AllFields };
			}

			q.DebugOptionGetIndexEntries = model.ShowEntries;
			
			q.SkipTransformResults = model.SkipTransformResults;
			if (model.IsSpatialQuerySupported &&
				model.Latitude.HasValue && model.Longitude.HasValue)
			{
				q = new SpatialIndexQuery(q)
						{
							QueryShape = SpatialIndexQuery.GetQueryShapeFromLatLon(model.Latitude.Value, model.Longitude.Value, model.Radius.HasValue ? model.Radius.Value : 1),
							SpatialRelation = SpatialRelation.Within,
							SpatialFieldName = Constants.DefaultSpatialFieldName,
							DefaultOperator = model.DefualtOperator
						};
			}
			return q;
		}

		private void SuggestResults()
		{
			foreach (var fieldAndTerm in model.GetCurrentFieldsAndTerms())
			{
				DatabaseCommands.SuggestAsync(model.IndexName, new SuggestionQuery {Field = fieldAndTerm.Field, Term = fieldAndTerm.Term, MaxSuggestions = 10})
					.ContinueOnSuccessInTheUIThread(result => model.Suggestions.AddRange(
						result.Suggestions.Select(term => new FieldAndTerm(fieldAndTerm.Field, fieldAndTerm.Term){SuggestedTerm = term})));
			}
		}
	}
}
