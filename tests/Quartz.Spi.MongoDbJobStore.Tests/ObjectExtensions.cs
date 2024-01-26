using System.Text.Json;

namespace Quartz.Util
{
    /// <summary>
    /// Generic extension methods for objects.
    /// </summary>
    public static class ObjectExtensions
    {
        /// <summary>
        /// Creates a deep copy of object by serializing to json.
        /// </summary>
        /// <param name="obj"></param>
        public static T DeepClone<T>(this T obj) where T : class
        {
            if (obj == null)
            {
                return null;
            }

            var clone = JsonSerializer.Serialize(obj);
            return JsonSerializer.Deserialize<T>(clone);
        }
    }
}
