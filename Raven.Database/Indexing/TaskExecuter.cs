using System;
using log4net;
using Raven.Database.Storage;
using Raven.Database.Tasks;

namespace Raven.Database.Indexing
{
    public class TaskExecuter
    {
        private readonly TransactionalStorage transactionalStorage;
        private readonly WorkContext context;
        private readonly ILog log = LogManager.GetLogger(typeof (TaskExecuter));

        public TaskExecuter(TransactionalStorage transactionalStorage, WorkContext context)
        {
            this.transactionalStorage = transactionalStorage;
            this.context = context;
        }

        public void Execute()
        {
            while(context.DoWork)
            {
                bool foundWork = false;
                transactionalStorage.Batch(actions =>
                {
                    var taskAsJson = actions.GetFirstTask();
                    if (taskAsJson == null)
                    {
                        actions.Commit();
                        return;
                    }
                    log.DebugFormat("Executing {0}", taskAsJson);
                    foundWork = true;

                    Task task;
                    try
                    {
                        task = Task.ToTask(taskAsJson);
                        try
                        {
                            task.Execute(context);
                        }
                        catch (Exception e)
                        {
                            if (log.IsWarnEnabled)
                            {
                                log.Warn(string.Format("Task {0} has failed and was deletedwithout completing any work", taskAsJson), e);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        log.Error("Could not create instance of a task from " + taskAsJson, e);
                    }

                    actions.CompleteCurrentTask();
                    actions.Commit();
                });
                if(foundWork == false)
                    context.WaitForWork();
            }
        }
    }
}