/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections.Generic;
using System.Runtime.Remoting.Lifetime;
using System.Text;

namespace Lucene.Net.Distributed.Search
{
    /// <summary>
    /// An derived implementation of RemoteSearchable, DistributedSearchable provides additional
    /// support for integration with .Net remoting objects and constructs.
    /// </summary>
    [Serializable]
    public class DistributedSearchable : Lucene.Net.Search.RemoteSearchable
    {

        private static readonly int TIME_FACTOR = 30;
        private static int initialLeaseTime = SupportClass.AppSettings.Get("LeaseTime", -1);
        private static TimeSpan leaseTimeSpan;

        /// <summary>
        /// Standard constructor for DistributedSearchable
        /// </summary>
        /// <param name="local">Any derived Searchable object</param>
        public DistributedSearchable(Lucene.Net.Search.Searchable local)
            : base(local)
        {
        }

        // Override on lifetime service; returning NULL yields an unlimited TTL
        /// <summary>
        /// Override of the base LifetimeService policy for this object.  This method
        /// manages the lifetime of objects marshaled and released in remoting environments
        /// and distributed garbage collection.
        /// </summary>
        /// <returns>Object of type ILease</returns>
        public override object InitializeLifetimeService()
        {
            DistributedSearchable.leaseTimeSpan = new TimeSpan(0, 0, DistributedSearchable.initialLeaseTime + DistributedSearchable.TIME_FACTOR);
			if (DistributedSearchable.initialLeaseTime == -1)
			{
				return null;		//Permanent TTL; never get's GC'd
			}
			else
			{
				ILease oLease = (ILease) base.InitializeLifetimeService();
				if (oLease.CurrentState == LeaseState.Initial)
				{
					oLease.InitialLeaseTime = TimeSpan.FromSeconds(DistributedSearchable.leaseTimeSpan.TotalSeconds);
					oLease.RenewOnCallTime = TimeSpan.FromSeconds(DistributedSearchable.TIME_FACTOR);
				}
				return oLease;
            }
        }

        /// <summary>
        /// Method to extend the lifetime of a DistributedSearchable object.
        /// </summary>
        public void Renew()
        {
            ILease oLease = (ILease)this.GetLifetimeService();
            oLease.Renew(DistributedSearchable.GetLeaseTimeSpan());
        }

        /// <summary>
        /// Retrieves the lifetime lease length composed as a TimeSpan object
        /// </summary>
        /// <returns>TimeSpan representing the length of the lifetime lease for this object</returns>
        public static TimeSpan GetLeaseTimeSpan()
        {
            return DistributedSearchable.leaseTimeSpan;
        }

        /// <summary>
        /// Returns the configured number of seconds for the lifetime length of this object
        /// </summary>
        /// <returns>int representing the configured number of seconds for the lifetime length of this object</returns>
        public static int GetInitialLeaseTime()
        {
            return DistributedSearchable.initialLeaseTime;
        }

    }
}
