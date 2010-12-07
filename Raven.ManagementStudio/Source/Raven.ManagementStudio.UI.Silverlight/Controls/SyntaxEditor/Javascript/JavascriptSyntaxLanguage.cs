using System;
using ActiproSoftware.Text.Implementation;
using ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.Common;
using ActiproSoftware.Windows.Controls.SyntaxEditor.IntelliPrompt.Implementation;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining;
using ActiproSoftware.Windows.Controls.SyntaxEditor.Outlining.Implementation;

namespace ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.QuickStart.CodeOutliningCollapsedText
{

    /// <summary>
    /// Implements a <c>Javascript</c> syntax language definition that support code outlining (folding).
    /// </summary>
    public class JavascriptSyntaxLanguage : SyntaxLanguage
    {

        /////////////////////////////////////////////////////////////////////////////////////////////////////
        // OBJECT
        /////////////////////////////////////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Initializes a new instance of the <c>JavascriptSyntaxLanguage</c> class.
        /// </summary>
        public JavascriptSyntaxLanguage()
            : base("Javascript")
        {
            // Initialize this language from a language definition
            SyntaxEditorHelper.InitializeLanguageFromResourceStream(this, "JScript.langdef");

            // Register an outliner, which tells the document's outlining manager that
            //   this language supports automatic outlining, and helps drive outlining updates
            this.RegisterService<IOutliner>(new TokenOutliner<JavascriptOutliningSource>());

            // Register a built-in service that automatically provides quick info tips 
            //   when hovering over collapsed outlining nodes
            this.RegisterService(new CollapsedRegionQuickInfoProvider());
        }

    }

}
