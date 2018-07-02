#region License
/* 
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not 
 * use this file except in compliance with the License. You may obtain a copy 
 * of the License at 
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0 
 *   
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the 
 * License for the specific language governing permissions and limitations 
 * under the License.
 * 
 */
#endregion

/*
 * Geniuslink (dba GeoRiot Networks) is using a modified version of this code from Quartz 2.x.
 * The original code uses .NET Framework remoting CallContext (and HttpContext in the alternative)
 * to share thread-local data. This code is being used to avoid a rewrite of Quartz.Spi.MongoDBJobStore
 * which requires the LogicalThreadContext class, which was removed in Quartz 3. Additionally, the
 * remoting feature is being removed in .NET Core, so we use a work around instead of the CallContext.
 */

using System.Collections.Concurrent;
using System.Security;

// Workaround for getting off remoting removed in NET Core: http://www.cazzulino.com/callcontext-netstandard-netcore.html
#if NET452
using System.Runtime.Remoting.Messaging;
#elif NET462 || NETSTANDARD2_0
using System.Threading;
#endif

namespace Quartz.Util
{
    /// <summary>
    /// Wrapper class to access thread local data.
    /// Data is either accessed from thread or HTTP Context's 
    /// data if HTTP Context is available.
    /// </summary>
    /// <author>Marko Lahma .NET</author>
    public static class LogicalThreadContext
    {
        /// <summary>
        /// Retrieves an object with the specified name.
        /// </summary>
        /// <param name="name">The name of the item.</param>
        /// <returns>The object in the call context associated with the specified name or null if no object has been stored previously</returns>

#if NET462 || NETSTANDARD2_0
        static ConcurrentDictionary<string, AsyncLocal<object>> state = new ConcurrentDictionary<string, AsyncLocal<object>>();
#endif

        public static T GetData<T>(string name)
        {
#if NET452
            return (T)CallContext.GetData(name);
#elif NET462 || NETSTANDARD2_0
            return state.TryGetValue(name, out AsyncLocal<object> data) ? (T)data.Value : default(T);
#endif
        }

        /// <summary>
        /// Stores a given object and associates it with the specified name.
        /// </summary>
        /// <param name="name">The name with which to associate the new item.</param>
        /// <param name="value">The object to store in the call context.</param>
        public static void SetData(string name, object value)
        {
#if NET452
            CallContext.SetData(name, value);
#elif NET462 || NETSTANDARD2_0
            state.GetOrAdd(name, _ => new AsyncLocal<object>()).Value = value;
#endif
        }

        /// <summary>
        /// Empties a data slot with the specified name.
        /// </summary>
        /// <param name="name">The name of the data slot to empty.</param>
        public static void FreeNamedDataSlot(string name)
        {
#if NET452
            CallContext.FreeNamedDataSlot(name);
#elif NET462 || NETSTANDARD2_0
            state.TryRemove(name, out AsyncLocal<object> discard);
#endif
        }
    }
}