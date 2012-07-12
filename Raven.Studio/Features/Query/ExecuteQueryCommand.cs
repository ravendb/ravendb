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
using Raven.Client.Connection;
using Raven.Client.Linq;
using Raven.Studio.Controls.Editors;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Client.Extensions;

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
			query = model.Query.Value;
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

            model.CollectionSource.UpdateQuery(model.IndexName, CreateTemplateQuery());  
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

	        if (model.IsSpatialQuerySupported &&
	            model.Latitude.HasValue && model.Longitude.HasValue)
	        {
	            q = new SpatialIndexQuery(q)
	                    {
	                        Latitude = model.Latitude.Value,
	                        Longitude = model.Longitude.Value,
	                        Radius = model.Radius.HasValue ? model.Radius.Value : 1,
	                    };
	        }
	        return q;
	    }

	    private void SuggestResults()
		{
			foreach (var fieldAndTerm in QueryEditor.GetCurrentFieldsAndTerms(model.Query.Value))
			{
				DatabaseCommands.SuggestAsync(model.IndexName, new SuggestionQuery {Field = fieldAndTerm.Field, Term = fieldAndTerm.Term, MaxSuggestions = 10})
					.ContinueOnSuccessInTheUIThread(result => model.Suggestions.AddRange(
						result.Suggestions.Select(term => new FieldAndTerm(fieldAndTerm.Field, fieldAndTerm.Term){SuggestedTerm = term})));
			}
		}
	}
}
