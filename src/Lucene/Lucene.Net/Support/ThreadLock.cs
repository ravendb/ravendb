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

using System.Threading;

namespace Lucene.Net.Support
{
    /// <summary>
    /// Abstract base class that provides a synchronization interface
    /// for derived lock types
    /// </summary>
    public abstract class ThreadLock
    {
        public abstract void Enter(object obj);
        public abstract void Exit(object obj);

        private static readonly ThreadLock _nullLock = new NullThreadLock();
        private static readonly ThreadLock _monitorLock = new MonitorThreadLock();
        
        /// <summary>
        /// A ThreadLock class that actually does no locking
        /// Used in ParallelMultiSearcher/MultiSearcher
        /// </summary>
        public static ThreadLock NullLock
        {
            get { return _nullLock; }
        }

        /// <summary>
        /// Wrapper class for the Monitor Enter/Exit methods
        /// using the <see cref="ThreadLock"/> interface
        /// </summary>
        public static ThreadLock MonitorLock
        {
            get { return _monitorLock; }
        }

        private sealed class NullThreadLock : ThreadLock
        {
            public override void Enter(object obj)
            {
                // Do nothing
            }

            public override void Exit(object obj)
            {
                // Do nothing
            }
        }

        private sealed class MonitorThreadLock : ThreadLock
        {
            public override void Enter(object obj)
            {
                Monitor.Enter(obj);
            }

            public override void Exit(object obj)
            {
                Monitor.Exit(obj);
            }
        }
    }
}
