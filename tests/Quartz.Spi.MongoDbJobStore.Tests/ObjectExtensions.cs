using Newtonsoft.Json;

namespace Quartz.Util
{
    /// <summary>
    /// Generic extension methods for objects.
    /// </summary>
    public static class ObjectExtensions
    {
        /// <summary>
        /// Creates a deep copy of object by serializing to memory stream.
        /// </summary>
        /// <param name="obj"></param>
        public static T DeepClone<T>(this T obj) where T : class
        {
            if (obj == null) return default(T);
            var clonedObj = JsonConvert.DeserializeObject<T>(JsonConvert.SerializeObject(obj));
            clonedObj = obj;
            return clonedObj;
        }
    }
}
