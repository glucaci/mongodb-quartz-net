using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.Json;

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
            if (obj == null)
            {
                return null;
            }
            
            using (MemoryStream ms = new MemoryStream())
            {
                JsonSerializer.Serialize(ms, obj);
                return JsonSerializer.Deserialize<T>(ms);
            }
        }
    }
}
