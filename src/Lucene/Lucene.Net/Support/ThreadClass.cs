/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

using System;
using System.Threading;
using Lucene.Net.Store;

namespace Lucene.Net.Support
{
    using System.Diagnostics;

    /// <summary>
    /// Support class used to handle threads
    /// </summary>
    public class ThreadClass : IThreadRunnable
    {
        /// <summary>
        /// The instance of System.Threading.Thread
        /// </summary>
        private System.Threading.Thread threadField;


        /// <summary>
        /// Initializes a new instance of the ThreadClass class
        /// </summary>
        public ThreadClass()
        {
            threadField = new System.Threading.Thread(new System.Threading.ThreadStart(Run));
        }

        /// <summary>
        /// Initializes a new instance of the Thread class.
        /// </summary>
        /// <param name="Name">The name of the thread</param>
        public ThreadClass(System.String Name)
        {
            threadField = new System.Threading.Thread(new System.Threading.ThreadStart(Run));
            this.Name = Name;
        }

        /// <summary>
        /// Initializes a new instance of the Thread class.
        /// </summary>
        /// <param name="Start">A ThreadStart delegate that references the methods to be invoked when this thread begins executing</param>
        public ThreadClass(System.Threading.ThreadStart Start)
        {
            threadField = new System.Threading.Thread(Start);
        }

        /// <summary>
        /// Initializes a new instance of the Thread class.
        /// </summary>
        /// <param name="Start">A ThreadStart delegate that references the methods to be invoked when this thread begins executing</param>
        /// <param name="Name">The name of the thread</param>
        public ThreadClass(System.Threading.ThreadStart Start, System.String Name)
        {
            threadField = new System.Threading.Thread(Start);
            this.Name = Name;
        }

        /// <summary>
        /// This method has no functionality unless the method is overridden
        /// </summary>
        public virtual void Run()
        {
        }

        /// <summary>
        /// Causes the operating system to change the state of the current thread instance to ThreadState.Running
        /// </summary>
        public virtual void Start()
        {
            threadField.Start();
        }

        /// <summary>
        /// Interrupts a thread that is in the WaitSleepJoin thread state
        /// </summary>
        public virtual void Interrupt()
        {
            threadField.Interrupt();
        }

        /// <summary>
        /// Gets the current thread instance
        /// </summary>
        public System.Threading.Thread Instance
        {
            get
            {
                return threadField;
            }
            set
            {
                threadField = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the thread
        /// </summary>
        public System.String Name
        {
            get
            {
                return threadField.Name;
            }
            set
            {
                if (threadField.Name == null)
                    threadField.Name = value;
            }
        }

        public void SetDaemon(bool isDaemon)
        {
            threadField.IsBackground = isDaemon;
        }


        /// <summary>
        /// Gets or sets a value indicating the scheduling priority of a thread
        /// </summary>
        public ThreadPriority Priority
        {
            get
            {
                try
                {
                    return threadField.Priority;
                }
                catch
                {
                    return ThreadPriority.Normal;
                }
            }
            set
            {
                try
                {
                    threadField.Priority = value;
                }
                catch { }

            }
        }

        /// <summary>
        /// Gets a value indicating the execution status of the current thread
        /// </summary>
        public bool IsAlive
        {
            get
            {
                return threadField.IsAlive;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether or not a thread is a background thread.
        /// </summary>
        public bool IsBackground
        {
            get
            {
                return threadField.IsBackground;
            }
            set
            {
                threadField.IsBackground = value;
            }
        }

        /// <summary>
        /// Blocks the calling thread until a thread terminates
        /// </summary>
        public void Join()
        {
            threadField.Join();
        }

        /// <summary>
        /// Blocks the calling thread until a thread terminates or the specified time elapses
        /// </summary>
        /// <param name="MiliSeconds">Time of wait in milliseconds</param>
        public void Join(long MiliSeconds)
        {
            var ts = new System.TimeSpan(MiliSeconds * 10000);
            threadField.Join(ts);
        }

        /// <summary>
        /// Blocks the calling thread until a thread terminates or the specified time elapses
        /// </summary>
        /// <param name="MiliSeconds">Time of wait in milliseconds</param>
        /// <param name="NanoSeconds">Time of wait in nanoseconds</param>
        public void Join(long MiliSeconds, int NanoSeconds)
        {
            var ts = new System.TimeSpan(MiliSeconds * 10000 + NanoSeconds * 100);
            threadField.Join(ts);
        }

        /// <summary>
        /// Resumes a thread that has been suspended
        /// </summary>
        public void Resume()
        {
            Monitor.PulseAll(threadField);
        }

        /// <summary>
        /// Raises a ThreadAbortException in the thread on which it is invoked, 
        /// to begin the process of terminating the thread. Calling this method 
        /// usually terminates the thread
        /// </summary>
        public void Abort()
        {
            threadField.Abort();
        }

        /// <summary>
        /// Raises a ThreadAbortException in the thread on which it is invoked, 
        /// to begin the process of terminating the thread while also providing
        /// exception information about the thread termination. 
        /// Calling this method usually terminates the thread.
        /// </summary>
        /// <param name="stateInfo">An object that contains application-specific information, such as state, which can be used by the thread being aborted</param>
        public void Abort(object stateInfo)
        {
            threadField.Abort(stateInfo);
        }

        /// <summary>
        /// Suspends the thread, if the thread is already suspended it has no effect
        /// </summary>
        public void Suspend()
        {
            Monitor.Wait(threadField);
        }

        /// <summary>
        /// Obtain a String that represents the current object
        /// </summary>
        /// <returns>A String that represents the current object</returns>
        public override System.String ToString()
        {
            return "Thread[" + Name + "," + Priority.ToString() + "]";
        }

        [ThreadStatic]
        static ThreadClass This = null;

        // named as the Java version
        public static ThreadClass CurrentThread()
        {
            return Current();
        }

        public static void Sleep(long ms)
        {
            // casting long ms to int ms could lose resolution, however unlikely
            // that someone would want to sleep for that long...
            Thread.Sleep((int)ms);
        }

        /// <summary>
        /// Gets the currently running thread
        /// </summary>
        /// <returns>The currently running thread</returns>
        public static ThreadClass Current()
        {
            if (This == null)
            {
                This = new ThreadClass();
                This.Instance = Thread.CurrentThread;
            }
            return This;
        }

        public static bool operator ==(ThreadClass t1, object t2)
        {
            if (((object)t1) == null) return t2 == null;
            return t1.Equals(t2);
        }

        public static bool operator !=(ThreadClass t1, object t2)
        {
            return !(t1 == t2);
        }

        public override bool Equals(object obj)
        {
            if (obj == null) return false;
            if (obj is ThreadClass) return this.threadField.Equals(((ThreadClass)obj).threadField);
            return false;
        }

        public override int GetHashCode()
        {
            return this.threadField.GetHashCode();
        }
    }
}
