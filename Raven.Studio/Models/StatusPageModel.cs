using Raven.Studio.Features.Stats;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Models
{
	public class StatusPageModel: PageViewModel
    {
		public StatusPageModel()
        {
            Status = new StatusModel();
			ModelUrl = "/Status";
        }

        public string CurrentDatabase
        {
            get { return ApplicationModel.Current.Server.Value.SelectedDatabase.Value.Name; }
        }

	    protected override void OnViewLoaded()
	    {
		    Status.Sections.Add(new StatisticsStatusSectionModel());
			Status.Sections.Add(new LogsStatusSectionModel());
			Status.Sections.Add(new AlertsStatusSectionModel());
			Status.Sections.Add(new IndexesErrorsStatusSectionModel());
			Status.Sections.Add(new ReplicationStatisticsStatusSectionModel());
			Status.Sections.Add(new UserInfoStatusSectionModel());
		    Status.SelectedSection.Value = Status.Sections[0];
	    }

	    public StatusModel Status { get; private set; }
    }
}
