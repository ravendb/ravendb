using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Database.Plugins
{
    public class DeleteTemporaryIndexes : IStartupTask
    {
        public void Execute(DocumentDatabase database)
        {
            // Delete any existing temporary indexes
            int totalIndexes = database.Statistics.CountOfIndexes;
            int pageSize = 128;
            int totalPages = (totalIndexes / pageSize) + 1;
            for (int currentPage = 0; currentPage < totalPages; currentPage++)
            {
                // Etc
            }
        }
    }
}
