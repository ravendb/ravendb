// -----------------------------------------------------------------------
//  <copyright file="ExecuteQueryCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
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

		    var documentHeight = model.DocumentsResult.Value != null
		                             ? model.DocumentsResult.Value.DocumentHeight
		                             : DocumentsModel.DefaultDocumentHeight;

			model.DocumentsResult.Value = new DocumentsModel(GetFetchDocumentsMethod)
			                              {
			                              	SkipAutoRefresh = true,
                                            ShowEditControls = false,
                                            ViewTitle = "Results",
                                            DocumentHeight = documentHeight,
			                              };
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
				Start = (model.Pager.CurrentPage - 1) * model.Pager.PageSize,
				PageSize = model.Pager.PageSize,
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

			if (model.IsSpatialQuerySupported)
			{
				q = new SpatialIndexQuery(q)
				    {
				    	Latitude = model.Latitude.HasValue ? model.Latitude.Value : 0,
						Longitude = model.Longitude.HasValue ? model.Longitude.Value : 0,
						Radius = model.Radius.HasValue ? model.Radius.Value : 0,
				    };
			}

			return DatabaseCommands.QueryAsync(model.IndexName, q, null)
				.ContinueOnSuccessInTheUIThread(qr =>
				{
					var viewableDocuments = qr.Results.Select(obj => new ViewableDocument(obj.ToJsonDocument())).ToArray();
					documentsModel.Documents.Match(viewableDocuments);
					documentsModel.Pager.TotalResults.Value = qr.TotalResults;

					if (qr.TotalResults == 0)
						SuggestResults();
				})
				.Catch(ex => model.Error = ex.ExtractSingleInnerException().SimplifyError());
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