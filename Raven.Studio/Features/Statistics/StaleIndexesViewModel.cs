using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;

namespace Raven.Studio.Features.Statistics
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Database;
	using Messages;
	using Raven.Database.Data;

	public class StaleIndexesViewModel : Screen, IHandle<StatisticsUpdated>
	{

		[ImportingConstructor]
		public StaleIndexesViewModel(IServer server)
		{
			DisplayName = "Stale Indexes";
			server.CurrentDatabaseChanged += delegate { NotifyOfPropertyChange(() => IndexStats); };
		}

		public IList<IndexStats> IndexStats {get; private set;}

		public void Handle(StatisticsUpdated message) { 
		
		message.Statistics.
		 }
	}
}
