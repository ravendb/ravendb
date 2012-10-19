using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Net;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Lexing;
using ActiproSoftware.Text.Utility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using Raven.Abstractions.Data;
using Raven.Studio.Features.Documents;
using Raven.Studio.Models;
using Raven.Abstractions.Extensions;
using System.Linq;

namespace Raven.Studio.Features.Patch
{
    public class PatchScriptIntelliPromptProvider : ICompletionProvider
    {
        private readonly IList<PatchValue> patchValues;
        private readonly IList<JsonDocument> recentDocuments;
        private bool documentPropertyCacheUpToDate;
        private List<string> documentProperties = new List<string>();
 
        public PatchScriptIntelliPromptProvider(IList<PatchValue> patchValues, ObservableCollection<JsonDocument> recentDocuments)
        {
            this.patchValues = patchValues;
            this.recentDocuments = recentDocuments;
            recentDocuments.CollectionChanged += delegate { documentPropertyCacheUpToDate = false; };
        }

        public string Key { get; private set; }
        public IEnumerable<Ordering> Orderings { get; private set; }

        
        public bool RequestSession(IEditorView view, bool canCommitWithoutPopup)
        {
            var session = new CompletionSession()
            {

            };

            var completionContext = GetCompletionContext(view);

            if (completionContext.IsDocumentProperty)
            {
                var properties = GetProperties(completionContext.CompletedPropertyPath);
                foreach (var property in properties)
                {
                    session.Items.Add(new CompletionItem()
                    {
                        ImageSourceProvider = new CommonImageSourceProvider(CommonImage.PropertyPublic),
                        Text = property,
                        AutoCompletePreText = property,
                        DescriptionProvider =
                            new HtmlContentProvider(string.Format("Document Property <em>{0}</em>", property))
                    }); 
                }
            }
            else if (!completionContext.IsObjectMember)
            {
                session.Items.Add(new CompletionItem()
                {
                    ImageSourceProvider = new CommonImageSourceProvider(CommonImage.MethodPublic),
                    Text = "LoadDocument",
                    AutoCompletePreText = "LoadDocument",
                    DescriptionProvider =
                        new HtmlContentProvider("(<em>documentId</em>)<br/>Loads the document with the given id")
                });

                foreach (var patchValue in patchValues)
                {
                    session.Items.Add(new CompletionItem()
                    {
                        ImageSourceProvider = new CommonImageSourceProvider(CommonImage.ConstantPublic),
                        Text = patchValue.Key,
                        AutoCompletePreText = patchValue.Key,
                        DescriptionProvider =
                            new HtmlContentProvider(
                                          string.Format("Script Parameter <em>{0}</em>. Current value: <em>{1}</em>",
                                                        patchValue.Key, patchValue.Value))
                    });
                }
            }

            if (session.Items.Count > 0)
            {
                session.Open(view);
                return true;
            }
            else
            {
                return false;
            }
        }

        private IEnumerable<string> GetProperties(string completedPropertyPath)
        {
            if (!documentPropertyCacheUpToDate)
            {
                documentProperties.Clear();
                documentProperties.AddRange(DocumentHelpers.GetPropertiesFromDocuments(recentDocuments, includeNestedProperties: true).Distinct());

                documentPropertyCacheUpToDate = true;
            }

            if (completedPropertyPath.Length > 0 && !completedPropertyPath.EndsWith("."))
            {
                completedPropertyPath += ".";
            }

            var prefixLength = completedPropertyPath.Length;
            var matchingProperties =
                documentProperties.Where(p => p.StartsWith(completedPropertyPath, StringComparison.InvariantCulture))
                    .Select(p =>
                    {
                        var trimmedString = p.Substring(prefixLength);
                        var indexOfDot = trimmedString.IndexOf('.');
                        trimmedString = indexOfDot >= 0 ? trimmedString.Substring(0, indexOfDot) : trimmedString;
                        return trimmedString;
                    })
                    .Distinct();

            return matchingProperties;
        }

        private CompletionContext GetCompletionContext(IEditorView view)
        {
            var reader = view.GetReader();

            var token = reader.ReadTokenReverse();
            var completionContext = new CompletionContext();

            if (token == null)
            {
                return completionContext;
            }

            var tokenText = reader.PeekText(token.Length);
            if (token.Key == "Identifier" || (token.Key == "Punctuation" && tokenText == "."))
            {
                var memberExpression = DetermineFullMemberExpression(tokenText, reader);

                if (memberExpression.Contains("."))
                {
                    completionContext.IsObjectMember = true;
                }

                if (memberExpression.StartsWith("this."))
                {
                    completionContext.IsDocumentProperty = true;
                    var completedPath = memberExpression.Substring("this.".Length);
                    var lastDot = completedPath.LastIndexOf('.');
                    completedPath = lastDot >= 0 ? completedPath.Substring(0, lastDot) : "";
                    completionContext.CompletedPropertyPath = completedPath;
                }
            }

            return completionContext;
        }

        private string DetermineFullMemberExpression(string tokenText, ITextSnapshotReader reader)
        {
            var sb = new StringBuilder(tokenText);

            var token = reader.ReadTokenReverse();
            while (token != null)
            {
                var text = reader.PeekText(token.Length);

                if (token.Key == "Identifier" || (token.Key == "Punctuation" && text == ".") || (token.Key == "Keyword" && text == "this"))
                {
                    sb.Insert(0, text);
                }
                else
                {
                    break;
                }

                token = reader.ReadTokenReverse();
            }

            return sb.ToString();
        }

        private class CompletionContext
        {
            public bool IsObjectMember;

            public bool IsDocumentProperty;

            public string CompletedPropertyPath = "";
        }
    }
}
