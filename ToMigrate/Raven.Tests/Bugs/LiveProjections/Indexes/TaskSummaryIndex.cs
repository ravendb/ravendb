using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Bugs.LiveProjections.Entities;

namespace Raven.Tests.Bugs.LiveProjections.Indexes
{
    public class TaskSummaryIndex : AbstractIndexCreationTask<Task, TaskSummary>
    {
        public TaskSummaryIndex()
        {
            Map = docs => from t in docs
                          select new { t.Start };

            IndexSortOptions.Add(s => s.Start, Raven.Abstractions.Indexing.SortOptions.String);
        }
    }

    public class TaskSummaryTransformer : AbstractTransformerCreationTask<Task>
    {
        public TaskSummaryTransformer()
        {

            TransformResults = results => from result in results
                                                      let giver = LoadDocument<Raven.Tests.Bugs.LiveProjections.Entities.User>("users/" + result.GiverId)
                                                      let taker = LoadDocument<Raven.Tests.Bugs.LiveProjections.Entities.User>("users/" + result.TakerId)
                                                      let place = LoadDocument<Place>("places/" + result.PlaceId)
                                                      select new
                                                      {
                                                          Id = result.Id,
                                                          Description = result.Description,
                                                          Start = result.Start,
                                                          End = result.End,
                                                          GiverId = result.GiverId,
                                                          GiverName = giver.Name,
                                                          TakerId = result.TakerId,
                                                          TakerName = taker != null ? taker.Name : null,
                                                          PlaceId = result.PlaceId,
                                                          PlaceName = place.Name
                                                      };
        }
    }
}
