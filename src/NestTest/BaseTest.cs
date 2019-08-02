using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nest;
using Nest7Demo;
using Shouldly;

namespace NestTest
{
    [TestClass]
    public class BaseTest
    {
        [TestMethod]
        public void TestConnection()
        {
            IElasticClient client= EsHelper.CreateElasticClient();
            PingResponse p= client.Ping();

            p.IsValid.ShouldBeTrue();
        }
    }
}
