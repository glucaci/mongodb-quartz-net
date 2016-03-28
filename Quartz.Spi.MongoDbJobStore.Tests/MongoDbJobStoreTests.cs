using NUnit.Framework;

namespace Quartz.Spi.MongoDbJobStore.Tests
{
    [TestFixture]
    public class MongoDbJobStoreTests : BaseStoreTests
    {
        [Test]
        public void TestStoreInitialization()
        {
            Assert.DoesNotThrow(() =>
            {
                CreateScheduler();
            });
        }
    }
}