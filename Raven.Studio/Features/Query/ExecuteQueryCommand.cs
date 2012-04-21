// -----------------------------------------------------------------------
//  <copyright file="ExecuteQueryCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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

		public override void Execute(object _)
		{
			query = model.Query.Value;
			ClearRecentQuery();
			model.RememberHistory();

			model.DocumentsResult.Value = new DocumentsModel
			                              {
			                              	SkipAutoRefresh = true,
			                              	ShowEditControls = false,
			                              	Header = "Results",
			                              	CustomFetchingOfDocuments = GetFetchDocumentsMethod, 
											Pager = {IsSkipBasedOnTheUrl = false},
			                              };
			model.DocumentsResult.Value.ForceTimerTicked();
		}

		private void ClearRecentQuery()
		{
			model.Error = null;
			model.Suggestions.Clear();
		}

		private Task GetFetchDocumentsMethod(DocumentsModel documentsModel)
		{
			var q = new IndexQuery
			{
				Start = model.DocumentsResult.Value.Pager.Skip,
				PageSize = model.DocumentsResult.Value.Pager.PageSize,
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

			var queryStartTime = DateTime.Now.Ticks;
			var queryEndtime = DateTime.MinValue.Ticks;
			return DatabaseCommands.QueryAsync(model.IndexName, q, null)
				.ContinueWith(task =>
				{
					queryEndtime = DateTime.Now.Ticks;
					return task;
				})
				.Unwrap()
				.ContinueOnSuccessInTheUIThread(qr =>
				{
					model.QueryTime = new TimeSpan(queryEndtime - queryStartTime);
					model.Results = new RavenQueryStatistics
					{
						IndexEtag = qr.IndexEtag,
						IndexName = qr.IndexName,
						IndexTimestamp = qr.IndexTimestamp,
						IsStale = qr.IsStale,
						SkippedResults = qr.SkippedResults,
						Timestamp = DateTime.Now,
						TotalResults = qr.TotalResults
					};
					var viewableDocuments = qr.Results.Select(obj => new ViewableDocument(obj.ToJsonDocument())).ToArray();
					
					var documetsIds = new List<string>();
					ProjectionData.Projections.Clear();
					foreach (var viewableDocument in viewableDocuments)
					{
						var id = string.IsNullOrEmpty(viewableDocument.Id) == false ? viewableDocument.Id : Guid.NewGuid().ToString();

						if (string.IsNullOrEmpty(viewableDocument.Id))
							ProjectionData.Projections.Add(id, viewableDocument);

						documetsIds.Add(id);

						viewableDocument.NeighborsIds = documetsIds;
					}
					
					documentsModel.Documents.Match(viewableDocuments);
					documentsModel.Pager.TotalResults.Value = qr.TotalResults;

					if (qr.TotalResults == 0)
						SuggestResults();
				})
				.CatchIgnore<WebException>(ex => model.Error = ex.SimplifyError());
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