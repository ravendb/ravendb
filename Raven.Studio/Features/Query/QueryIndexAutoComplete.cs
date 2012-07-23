// -----------------------------------------------------------------------
//  <copyright file="QueryIndexAutoComplete.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using ActiproSoftware.Text;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Raven.Studio.Extensions;

namespace Raven.Studio.Features.Query
{
	public class QueryIndexAutoComplete : NotifyPropertyChangedBase
	{
		private readonly string indexName;
		private readonly IEditorDocument queryDocument;

		private readonly BindableCollection<string> fields = new BindableCollection<string>(x => x);
		private readonly Dictionary<string, Dictionary<string, List<string>>> fieldsTermsDictionary =
			new Dictionary<string, Dictionary<string, List<string>>>();

		private ICompletionProvider completionProvider;

	    public ICompletionProvider CompletionProvider
		{
			get { return completionProvider; }
			set
			{
				completionProvider = value;
				OnPropertyChanged(() => CompletionProvider);
			}
		}

        public QueryIndexAutoComplete(IList<string> fields) : this(fields, null, null)
        {
            
        }

		public QueryIndexAutoComplete(IList<string> fields, string indexName, IEditorDocument queryDocument)
		{
            if (indexName != null && queryDocument != null)
            {
                this.indexName = indexName;
                this.queryDocument = queryDocument;
                queryDocument.ObserveTextChanged()
                    .Throttle(TimeSpan.FromSeconds(0.2))
                    .ObserveOnDispatcher<EventPattern<TextSnapshotChangedEventArgs>>()
                    .SubscribeWeakly(this, (target, _) => target.GetTermsForUsedFields());

                CompletionProvider = new QueryIntelliPromptProvider(fields, indexName, fieldsTermsDictionary);
            }
            else
            {
                CompletionProvider = new QueryIntelliPromptProvider(fields, null, null);
            }

		    this.fields.Match(fields);
		}

		private void GetTermsForUsedFields()
		{
		    var fields = queryDocument.GetTextOfAllTokensMatchingType("Field")
		        .Select(t => t.TrimEnd(':').Trim())
                .Except(fieldsTermsDictionary.Keys)
		        .ToList();

            foreach (var field in fields)
			{
				var termsDictionary = fieldsTermsDictionary[field] = new Dictionary<string, List<string>>();
				var terms = termsDictionary[string.Empty] = new List<string>();

				GetTermsForFieldAsync(indexName, field, terms);
			}
		}

		public static Task GetTermsForFieldAsync(string indexName, string field, List<string> terms, string termPrefix = "")
		{
		    return ApplicationModel.DatabaseCommands.GetTermsAsync(indexName, field, termPrefix, 1024)
		        .ContinueOnSuccessInTheUIThread(
		            results =>
		                {
		                    terms.Clear();
		                    terms.AddRange(results);
		                });
		}
	}
}
