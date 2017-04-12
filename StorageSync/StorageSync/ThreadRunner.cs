using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Diagnostics;

namespace StorageSync
{
    internal class ThreadRunner
    {
        private List<ThreadRunnerThread> toDoList;
        private int activeThreadCount;
        private ThreadRunnerThread toDoItem;
        private AutoResetEvent autoEvent;
        public delegate void TaskDelegate(string toDoItem);

        public void Run(IEnumerable<ThreadRunnerThread> toDoList, int maxThreads)
        {
            this.toDoList = new List<ThreadRunnerThread>(toDoList);
            this.activeThreadCount = 0;

            for (int i = 0; i < maxThreads; i++)
            {
                this.activeThreadCount++;

                toDoItem = GetNextItem();

                if (toDoItem != null && toDoItem.Task != null)
                {
                    toDoItem.Task.BeginInvoke(toDoItem.Item, TaskComplete, toDoItem);
                }
            }

            // Wait until all threads complete
            this.autoEvent = new AutoResetEvent(false);
            this.autoEvent.WaitOne();
        }

        private void TaskComplete(IAsyncResult result)
        {
            var thread = (ThreadRunnerThread)result.AsyncState;

            try
            {
                thread.Task.EndInvoke(result);
            }
            catch (Exception e)
            {
                Trace.TraceWarning("{0:yyyy MM dd hh:mm:ss} Backup task {1} failed with error {2}", DateTime.UtcNow, thread.Item, e.Message);
            }

            // Are there any items left? 
            if (this.toDoList.Count == 0)
            {
                activeThreadCount--;

                if (activeThreadCount <= 0)
                {
                    // End the block in the Run method
                    this.autoEvent.Set();
                }
            }
            else
            {
                // Move onto the next available item
                var nextItem = GetNextItem();
                nextItem.Task.BeginInvoke(nextItem.Item, TaskComplete, nextItem);
            }
        }

        private ThreadRunnerThread GetNextItem()
        {
            var itemName = this.toDoList.FirstOrDefault();

            if (itemName != null)
            {
                this.toDoList.Remove(itemName);
            }

            return itemName;
        }

        public class ThreadRunnerThread
        {
            public ThreadRunnerThread(string item, TaskDelegate task, int priority)
            {
                this.Item = item;
                this.Task = task;
                this.Priority = priority;
            }
            
            public string Item { get; private set; }
            public TaskDelegate Task { get; private set; }
            public int Priority { get; private set; }
        }
    }
}