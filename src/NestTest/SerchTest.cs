using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nest;
using Nest7Demo;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NestTest
{
    [TestClass]
    public class SerchTest
    {
        [TestMethod]
        public void TestScriptSearch()
        {
            IElasticClient client = EsHelper.CreateElasticClient();
            client.Search<TestModel>(s => s.Query(q => q.Bool(b => b.Must(m => m.Script(st => st.Script(spt => spt.Source("").Lang("")))))));
        }
        
    }
}
