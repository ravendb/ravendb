using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Parsing;
using ActiproSoftware.Text.Utility;
using ActiproSoftware.Windows.Controls.SyntaxEditor;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Features.JsonEditor
{
    public class DocumentReferencesFinder : ICodeDocumentPropertyChangeEventSink
    {
        public void NotifyFileNameChanged(ICodeDocument document, StringPropertyChangedEventArgs e)
        {
            
        }

        public void NotifyParseDataChanged(ICodeDocument document, ParseDataPropertyChangedEventArgs e)
        {
            var documentIdManager = document.Properties.GetOrCreateSingleton(() => new DocumentReferencedIdManager());
            UpdateReferences(document, documentIdManager);
        }

        private void UpdateReferences(ICodeDocument document, DocumentReferencedIdManager idManager)
        {
            // Note: if this proves to be too slow with large documents, we can potentially optimize the finding 
            // of references by only considering the parts of the AST which occur after the Offset at which the text change began
            // (we can find this by getting hold of the TextSnapshotChangedEventArgs)
            var potentialReferences = FindPotentialReferences(document).ToList();
            var newReferences = potentialReferences.Where(idManager.NeedsChecking).ToArray();

            if (newReferences.Any())
            {
               DocumentIdCheckHelpers.GetActualIds(newReferences)
                                .ContinueOnSuccessInTheUIThread(ids =>
                                {
                                    idManager.AddKnownIds(ids);
                                    idManager.AddKnownInvalidIds(newReferences.Except(ids));

                                    idManager.UpdateCurrentIds(potentialReferences);
                                });
            }
        }

        private IEnumerable<string> FindPotentialReferences(ICodeDocument codeDocument)
        {
            var stringValueNodes = codeDocument.FindAllStringValueNodes();
            return stringValueNodes.Select(n => n.Text).Distinct().Where(DocumentIdCheckHelpers.IsPotentialId);
        }
    }
}
