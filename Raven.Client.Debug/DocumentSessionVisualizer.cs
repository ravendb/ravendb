using System.Diagnostics;
using Microsoft.VisualStudio.DebuggerVisualizers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows.Forms;
using Raven.Client.Connection.Profiling;
using Raven.Client.Debug;
using Raven.Client.Document;

[assembly: DebuggerVisualizer(typeof(DocumentSessionVisualizer), typeof(DocumentSessionVisualizerObjectSource), Target = typeof(DocumentSession))]

namespace Raven.Client.Debug
{
	public class DocumentSessionVisualizer : DialogDebuggerVisualizer
	{
		protected override void Show(IDialogVisualizerService windowService, IVisualizerObjectProvider objectProvider)
		{
			if (windowService == null)
				throw new ArgumentNullException("windowService");
			if (objectProvider == null)
				throw new ArgumentNullException("objectProvider");

			var profilingInformation = (ProfilingInformation)objectProvider.GetObject();
			using (var displayForm = new DocumentSessionView
			{
				ProfilingInformation = profilingInformation
			})
			{
				displayForm.Text = profilingInformation.At.ToString();
				windowService.ShowDialog(displayForm);
			}
		}

		public static void Display(IDocumentSession objectToVisualize)
		{
			var visualizerHost = new VisualizerDevelopmentHost(objectToVisualize, typeof(DocumentSessionVisualizer), typeof(DocumentSessionVisualizerObjectSource));
			visualizerHost.ShowVisualizer();
		}
	}
}
