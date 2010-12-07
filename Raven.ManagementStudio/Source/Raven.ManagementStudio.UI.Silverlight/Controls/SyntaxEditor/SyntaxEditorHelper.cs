using System;
using System.IO;
using System.Reflection;
using System.Xml;
using ActiproSoftware.Text;
using ActiproSoftware.Text.Implementation;

namespace ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.Common
{

    /// <summary>
    /// Provides some helper methods.
    /// </summary>
    public static class SyntaxEditorHelper
    {

        public const string DefinitionPath = "Raven.ManagementStudio.UI.Silverlight.Controls.SyntaxEditor.Definitions.";
        public const string XmlDocumentsPath = "ActiproSoftware.Windows.ProductSamples.SyntaxEditorSamples.Languages.XmlDocuments.";

        /// <summary>
        /// Initializes an existing <see cref="ISyntaxLanguage"/> from a language definition (.langdef file) from a resource stream.
        /// </summary>
        /// <param name="filename">The filename.</param>
        public static void InitializeLanguageFromResourceStream(ISyntaxLanguage language, string filename)
        {
            string path = DefinitionPath + filename;
            using (Stream stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(path))
            {
                if (stream != null)
                {
                    SyntaxLanguageDefinitionSerializer serializer = new SyntaxLanguageDefinitionSerializer();
                    serializer.InitializeFromStream(language, stream);
                }
            }
        }

        /// <summary>
        /// Loads a language definition (.langdef file) from a resource stream.
        /// </summary>
        /// <param name="filename">The filename.</param>
        /// <returns>The <see cref="ISyntaxLanguage"/> that was loaded.</returns>
        public static ISyntaxLanguage LoadLanguageDefinitionFromResourceStream(string filename)
        {
            string path = DefinitionPath + filename;
            using (Stream stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(path))
            {
                if (stream != null)
                {
                    SyntaxLanguageDefinitionSerializer serializer = new SyntaxLanguageDefinitionSerializer();
                    return serializer.LoadFromStream(stream);
                }
                else
                    return SyntaxLanguage.PlainText;
            }
        }

    }
}
