using System;

namespace Quartz.Spi.MongoDbJobStore
{
    /// <summary>
    ///     Attribute used to annotate Enities with to override mongo collection name. By default, when this attribute
    ///     is not specified, the classname will be used.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    internal class CollectionName : Attribute
    {
        /// <summary>
        ///     Initializes a new instance of the CollectionName class attribute with the desired name.
        /// </summary>
        /// <param name="name">Id of the collection.</param>
        public CollectionName(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentException("Empty collectionname not allowed", nameof(name));
            }

            Name = name;
        }

        /// <summary>
        ///     Gets the name of the collection.
        /// </summary>
        /// <name>The name of the collection.</name>
        public string Name { get; private set; }
    }
}