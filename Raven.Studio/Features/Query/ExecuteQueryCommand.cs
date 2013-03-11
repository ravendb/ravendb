// -----------------------------------------------------------------------
//  <copyright file="ExecuteQueryCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Studio.Extensions;

namespace Raven.Studio.Features.Query
{
	public class ExecuteQueryCommand : Command
	{
		private readonly QueryModel model;

		public ExecuteQueryCommand(QueryModel model)
		{
			this.model = model;
		}

		public override void Execute(object parameter)
		{
			ClearRecentQuery();
			model.RememberHistory();

			Observable.FromEventPattern<QueryStatisticsUpdatedEventArgs>(
				h => model.CollectionSource.QueryStatisticsUpdated += h, h => model.CollectionSource.QueryStatisticsUpdated -= h)
				.Take(1)
				.ObserveOnDispatcher()
				.Subscribe(e =>
				{
					if (e.EventArgs.Statistics.TotalResults == 0)
					{
						SuggestResults();
					}
				});

            model.DocumentsResult.SetPriorityColumns(GetRelevantFields());
		    var templateQuery = model.CreateTemplateQuery();
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